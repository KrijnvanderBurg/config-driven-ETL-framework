

## Triggers

Triggers control WHEN alerts fire and WHERE they're sent. Each trigger evaluates its rules against exceptions. If ALL rules match (AND logic), the trigger fires and sends alerts to all channels in `channel_names`.

**Execution flow**:
1. Check if trigger is `enabled` (skip if `false`)
2. Evaluate all rules against exception (skip if any rule returns `false`)
3. Format title and body using template
4. Find each channel by name in `channel_names`
5. Call channel's `alert()` method with formatted title/body

**Required fields**:
- `name`: Unique trigger identifier
- `enabled`: Boolean flag to activate/deactivate trigger
- `channel_names`: Array of channel names to notify (must match channel `name` fields exactly)
- `template`: Message formatting configuration (all 4 fields required):
  - `prepend_title`: String prepended to title
  - `append_title`: String appended to title
  - `prepend_body`: String prepended to body
  - `append_body`: String appended to body
- `rules`: Array of rule objects (empty array `[]` means always fire)

**Optional fields**:
- `description`: Documentation string (default: `""`)

```jsonc
{
    "name": "prod-critical",
    "enabled": true,
    "description": "Production critical errors route to ops team",
    "channel_names": ["ops-email", "slack-webhook"],
    "template": {
        "prepend_title": "🚨 PROD: ",
        "append_title": "",
        "prepend_body": "Critical error detected:\n",
        "append_body": "\n\nInvestigate immediately."
    },
    "rules": [
        {"rule": "exception_regex", "pattern": ".*database.*"},
        {"rule": "env_vars_matches", "env_var_name": "ENVIRONMENT", "env_var_values": ["production"]}
    ]
}
```

### Rules

Rules are conditions evaluated against exceptions. ALL rules in a trigger must return `true` for the trigger to fire (AND logic). For OR logic, create separate triggers.

**Exception Regex Rule**

Matches exception message against regex pattern using `re.search()`.

- `rule`: Must be literal `"exception_regex"`
- `pattern`: Regex pattern string (matched against `str(exception)`)

Returns `true` if pattern matches exception message, `false` otherwise. Empty pattern returns `true`.

```jsonc
{"rule": "exception_regex", "pattern": ".*database.*|.*timeout.*|.*connection.*"}
```

**Environment Variable Match Rule**

Checks if environment variable has one of the expected values using `os.environ.get()`.

- `rule`: Must be literal `"env_vars_matches"`
- `env_var_name`: Environment variable name to check
- `env_var_values`: Array of acceptable values

Returns `true` if `os.environ[env_var_name]` equals any value in `env_var_values`, `false` otherwise. If env var doesn't exist, returns `false`. Empty `env_var_name` returns `true`.

```jsonc
{"rule": "env_vars_matches", "env_var_name": "ENVIRONMENT", "env_var_values": ["production", "prod"]}
```

**No Rules (Always Fire)**

Empty rules array means trigger fires for every exception:

```jsonc
"rules": []
```