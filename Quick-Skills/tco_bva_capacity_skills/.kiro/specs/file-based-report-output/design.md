# Technical Design: File-Based Report Output

## Overview

Remove `detail_level` from `calculate_agent_session_compounded_cost()`. The function always computes the full result, writes the detailed markdown report to a file, and returns a compact summary including `capacity_profile` for downstream capacity planning.

## Architecture

### New Data Flow

```
calculate_agent_session_compounded_cost(main_agent_config, subagents, output_path=None)
    │
    ├── [existing] compute full cost result (all detail)
    │
    ├── [new] _write_report_to_file(result, main_agent_config, ...)
    │       ├── resolve output_dir from config/default
    │       ├── generate filename from template
    │       ├── build YAML front-matter
    │       ├── format report body via _format_full_output()
    │       ├── write to disk
    │       ├── optionally run auto_cleanup
    │       └── return file_path (or None on failure)
    │
    └── [new] _build_compact_summary(result, file_path)
            └── return {file_path, monthly_total, capacity_profile, ...}
```

### Before vs After

| Aspect | Before | After |
|--------|--------|-------|
| Parameters | `detail_level="summary"` or `"full"` | `output_path=None` (optional) |
| Return (was summary) | ~10 keys, no file | ~12 keys + capacity_profile + file_path |
| Return (was full) | ~500 lines of nested dicts | Same compact summary (detail is in the file) |
| File output | Never | Always |
| capacity_profile | Only in full mode return | Always in summary |

## Components

### 1. `_sanitize_filename(name: str) -> str`

```python
def _sanitize_filename(name):
    """Convert string to filesystem-safe slug.
    'Claude Sonnet 4.6' → 'claude-sonnet-4.6'
    'Nova Lite (v2)' → 'nova-lite-v2'
    '' → 'unknown-model'
    """
    if not name:
        return "unknown-model"
    import re
    slug = name.lower().strip()
    slug = re.sub(r'[^a-z0-9.\-]', '-', slug)
    slug = re.sub(r'-+', '-', slug)
    return slug.strip('-') or "unknown-model"
```

### 2. `_format_volume(sessions_per_month: int) -> str`

```python
def _format_volume(sessions_per_month):
    """10000 → '10k-sessions', 1500000 → '1m-sessions', 500 → '500-sessions'"""
    if sessions_per_month >= 1_000_000:
        return f"{sessions_per_month // 1_000_000}m-sessions"
    elif sessions_per_month >= 1_000:
        return f"{sessions_per_month // 1_000}k-sessions"
    else:
        return f"{sessions_per_month}-sessions"
```

### 3. `_generate_report_path(model_name, sessions_per_month, output_dir=None, output_path=None) -> str`

```python
def _generate_report_path(model_name, sessions_per_month, output_dir=None, output_path=None):
    """Resolve full file path for the report."""
    if output_path:
        return os.path.abspath(os.path.expanduser(output_path))
    
    if output_dir is None:
        output_dir = resolve_setting("reports", "output_dir")
    output_dir = os.path.expanduser(output_dir)
    
    template = resolve_setting("reports", "naming_template")
    random_hex = os.urandom(2).hex()  # 4-char hex to prevent same-second collisions
    timestamp = time.strftime("%Y%m%d-%H%M%S") + f"-{random_hex}"
    model_slug = _sanitize_filename(model_name or "")
    volume_slug = _format_volume(sessions_per_month or 0)
    
    filename = template.format(
        model=model_slug, volume=volume_slug, timestamp=timestamp,
        region="", format="md",
    )
    return os.path.join(output_dir, filename)
```

### 4. `_build_front_matter(result, main_agent_config) -> str`

```python
def _build_front_matter(result, main_agent_config):
    """Build YAML front-matter. Returns '' if reports.include_metadata is False."""
    if not resolve_setting("reports", "include_metadata"):
        return ""
    
    import hashlib
    inputs_str = json.dumps(main_agent_config, sort_keys=True, default=str)
    inputs_hash = hashlib.sha256(inputs_str.encode()).hexdigest()[:16]
    
    lines = ["---"]
    lines.append(f'generated_at: "{time.strftime("%Y-%m-%dT%H:%M:%S")}"')
    lines.append(f'model: "{main_agent_config.get("model_name", "unknown")}"')
    lines.append(f'region: "{main_agent_config.get("region", "unknown")}"')
    lines.append(f'sessions_per_month: {main_agent_config.get("agent_sessions_per_month", 0)}')
    lines.append(f'session_total: {round(result.get("session_total", 0), 6)}')
    lines.append(f'monthly_total: {round(result.get("monthly_total", 0), 2)}')
    lines.append(f'annual_total: {round(result.get("annual_total", 0), 2)}')
    lines.append(f'savings_pct: {round(result.get("savings_pct", 0), 1)}')
    lines.append(f'inputs_hash: "{inputs_hash}"')
    lines.append("---")
    lines.append("")
    return "\n".join(lines)
```

### 5. `_write_report_to_file(result, main_agent_config, subagents=None, output_path=None) -> str | None`

```python
def _write_report_to_file(result, main_agent_config, subagents=None, output_path=None):
    """Write full report to markdown file. Returns path on success, None on failure.
    
    Cascade: tries output_path → default dir → returns None.
    Uses atomic write (temp file + rename) to prevent partial writes.
    """
    model_name = main_agent_config.get("model_name", "unknown")
    sessions = main_agent_config.get("agent_sessions_per_month", 0)
    
    file_path = _generate_report_path(model_name, sessions, output_path=output_path)
    
    # Build content first (before touching filesystem)
    front_matter = _build_front_matter(result, main_agent_config)
    try:
        body = _format_full_output(result)
    except Exception:
        body = result.get("token_table", "") + "\n\n" + json.dumps(
            result.get("explanation", {}), indent=2, default=str)
    content = front_matter + body
    
    # Try writing — cascade: configured path → default dir
    written_path = _try_write(file_path, content)
    if written_path is None and output_path:
        # Configured/explicit path failed — try default directory
        default_path = _generate_report_path(model_name, sessions)
        written_path = _try_write(default_path, content)
    
    if written_path and resolve_setting("reports", "auto_cleanup"):
        _cleanup_old_reports()
    
    return written_path


def _try_write(file_path, content):
    """Attempt atomic write. Returns absolute path on success, None on failure."""
    target_dir = os.path.dirname(file_path)
    try:
        os.makedirs(target_dir, exist_ok=True)
    except OSError as e:
        print(f"⚠️  Report: Cannot create directory '{target_dir}': {e}", file=sys.stderr)
        return None
    
    # Atomic write: write to temp file, then rename
    tmp_path = file_path + ".tmp"
    try:
        with open(tmp_path, "w") as f:
            f.write(content)
        os.rename(tmp_path, file_path)
    except OSError as e:
        print(f"⚠️  Report: Cannot write '{file_path}': {e}", file=sys.stderr)
        # Clean up temp file if it exists
        try:
            os.unlink(tmp_path)
        except OSError:
            pass
        return None
    
    return os.path.abspath(file_path)
```

### 6. `_build_compact_summary(result, file_path) -> dict`

```python
def _build_compact_summary(result, file_path):
    """Build the compact summary returned to the agent."""
    # Calculate savings_pct
    no_cache = result.get("session_total_no_cache", 0)
    with_cache = result.get("session_total", 0)
    savings_pct = ((no_cache - with_cache) / no_cache * 100) if no_cache > 0 else 0
    
    # Safe access to main agent session cost (prompt-cache-aware)
    main = result.get("main_agent", {})
    cached = main.get("with_cache") or main.get("no_cache") or {}
    main_session_cost = cached.get("session_total", 0)  # with prompt cache if available
    
    summary = {
        "file_path": file_path,
        "sessions_per_month": result.get("sessions_per_month", 0),
        "monthly_total": round(result.get("monthly_total", 0), 2),
        "annual_total": round(result.get("annual_total", 0), 2),
        "session_total": round(result.get("session_total", 0), 6),
        "session_total_no_cache": round(result.get("session_total_no_cache", 0), 6),
        "savings_pct": round(savings_pct, 1),
        "main_agent_session_cost": round(main_session_cost, 6),
        "subagent_session_cost": round(
            sum(sa.get("session_cost", 0) for sa in result.get("subagents", [])), 6),
        "recommended_ttl": main.get("recommended_ttl"),
        "top_cost_driver": _identify_top_cost_driver(result),
        "capacity_profile": result.get("capacity_profile"),
    }
    
    return summary
```

### 7. `_identify_top_cost_driver(result) -> str`

```python
def _identify_top_cost_driver(result):
    """Identify the largest cost component."""
    main = result.get("main_agent", {})
    # "with_cache" = prompt-cached cost, "no_cache" = without prompt caching
    main_cost = (main.get("with_cache") or main.get("no_cache") or {}).get("session_total", 0)
    subagents = result.get("subagents", [])
    sub_total = sum(sa.get("session_cost", 0) for sa in subagents)
    
    if sub_total > main_cost and subagents:
        top_sa = max(subagents, key=lambda x: x.get("session_cost", 0))
        return f"sub-agent ({top_sa.get('type', 'unknown')})"
    return "main agent (token compounding)"
```

### 8. `_cleanup_old_reports(output_dir=None, max_age_days=None) -> dict`

```python
def _cleanup_old_reports(output_dir=None, max_age_days=None):
    """Delete report files older than retention threshold.
    
    Only deletes files matching the naming template pattern (not arbitrary .md files).
    Derives the match pattern from reports.naming_template at cleanup time so it
    adapts if the template changes.
    """
    if output_dir is None:
        output_dir = os.path.expanduser(resolve_setting("reports", "output_dir"))
    if max_age_days is None:
        max_age_days = resolve_setting("reports", "retention_days")
    
    if not os.path.isdir(output_dir):
        return {"deleted_count": 0, "freed_bytes": 0}
    
    # Derive filename pattern from template
    # Default template: "{model}_{volume}_{timestamp}.md"
    # Pattern: any slug _ any slug _ timestamp-hex .md
    import re
    template = resolve_setting("reports", "naming_template")
    # Replace placeholders with regex wildcards
    pattern = template.replace("{model}", r"[a-z0-9.\-]+")
    pattern = pattern.replace("{volume}", r"[a-z0-9\-]+")
    pattern = pattern.replace("{timestamp}", r"\d{8}-\d{6}-[a-f0-9]{4}")
    pattern = pattern.replace("{region}", r"[a-z0-9\-]*")
    pattern = pattern.replace("{format}", r"[a-z]+")
    report_re = re.compile(f"^{pattern}$")
    
    cutoff = time.time() - (max_age_days * 86400)
    deleted = 0
    freed = 0
    
    for f in os.listdir(output_dir):
        if not report_re.match(f):
            continue
        path = os.path.join(output_dir, f)
        if not os.path.isfile(path):
            continue
        if os.path.getmtime(path) < cutoff:
            size = os.path.getsize(path)
            try:
                os.unlink(path)
                deleted += 1
                freed += size
            except OSError:
                pass
    
    return {"deleted_count": deleted, "freed_bytes": freed}
```

### 9. Changes to `calculate_agent_session_compounded_cost()`

**Signature change:**
```python
def calculate_agent_session_compounded_cost(
    main_agent_config,
    subagents=None,
    output_path=None,   # NEW: explicit file path (optional)
):
    # Remove: detail_level parameter entirely
```

**End of function (replaces existing return logic):**
```python
    # ── Write report to file ──
    file_path = _write_report_to_file(result, main_agent_config, subagents, output_path)
    
    if file_path is None:
        # File write failed — fall back to full inline result
        result["_file_write_failed"] = True
        return result
    
    # ── Build and return compact summary ──
    return _build_compact_summary(result, file_path)
```

The internal computation remains unchanged — we still compute `result` with all the detail (token_table, capacity_profile, per-cycle breakdowns, etc.). We just don't return it directly anymore.

### 10. Internal callers that need the full result

`_format_full_output()` is called inside `_write_report_to_file()` with the full `result` dict. No external caller needs the full dict anymore.

If tests need to verify internal computation, they can call the sub-functions directly:
- `calculate_main_agent_compounded_cost()` — still returns full detail
- `calculate_rag_subagent_tokens()` — still returns full detail
- `check_capacity_fit()` — unchanged

### 11. CLI: `--cleanup-reports`

```python
parser.add_argument("--cleanup-reports", action="store_true",
                    help="Delete report files older than retention_days (default 30)")

if args.cleanup_reports:
    result = _cleanup_old_reports()
    print(f"Deleted {result['deleted_count']} report(s), freed {result['freed_bytes'] / 1024:.1f} KB")
    return
```

## File Changes

| File | Change |
|------|--------|
| `bedrock_pricing.py` | Add ~120 lines (helpers + file writer + summary builder + cleanup). Remove `detail_level` param and its branching logic from `calculate_agent_session_compounded_cost()`. Remove `behavior.detail_level` from CONFIG_SCHEMA. |
| `bedrock-pricing/SKILL.md` | Remove `detail_level` references. Add "Report Output" section. Update examples and Output Structure. |
| `tests/test_report_output.py` | New test file (~150 lines) |
| `tests/test_config.py` | Update assertions that reference `detail_level` or old return structure. Update CONFIG_SCHEMA section count if `behavior.detail_level` is removed. |

## Error Handling

| Scenario | Behavior |
|----------|----------|
| Output dir doesn't exist | Create it |
| Output dir not writable | Try default dir (`~/bedrock_reports/`), if that also fails → inline fallback with warning |
| Disk full / write fails | Try default dir, if that also fails → inline fallback with warning |
| `output_path` not writable | Try default dir, if that also fails → inline fallback with warning |
| `_format_full_output()` fails | Write simplified report (token_table + JSON) |
| Cleanup dir doesn't exist | Return `{deleted_count: 0, freed_bytes: 0}` |

**Inline fallback warning (emitted to stderr):**
```
⚠️  Report: Failed to write report to '/path/file.md'. Returning full result inline.
    This increases token usage and latency. Specify a writable folder via
    reports.output_dir in ~/.bedrock_skills/config.yaml or pass output_path='/writable/path/report.md'.
```
