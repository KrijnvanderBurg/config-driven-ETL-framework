

## Triggers

Triggers control WHEN alerts fire and WHERE they're sent. Each trigger evaluates its rules against exceptions. If ALL rules match (AND logic), the trigger fires and sends alerts to all [channels](channels.md) listed in `channel_names`.

When designing your alerting strategy, remember that within a single trigger, rules use AND logic (all must match). For OR logic, create separate triggers—any matching trigger will fire. This means you can have multiple triggers evaluating the same exception, each with their own [channel routing](channels.md) and message [templates](#template-formatting), allowing different teams to receive differently formatted messages about the same error.

**Execution flow**:
1. Check if trigger is `enabled` (skip if `false`)
2. Evaluate all rules against exception (skip if any rule returns `false`)
3. Format title and body using template
4. Find each [channel by name](channels.md#overview) in `channel_names`
5. Call channel's `alert()` method with formatted title/body

**Configuration**:

```jsonc
{
    "name": "prod-critical",               // Must be unique within configuration
    "enabled": true,                       // Boolean to activate/deactivate
    "description": "Production critical errors route to ops team",
    "channel_names": ["ops-email", "slack-webhook"],  // Must match channel name fields exactly
    "template": {
        "prepend_title": "🚨 PROD: ",
        "append_title": "",                // All 4 template fields required (can be empty "")
        "prepend_body": "Critical error detected:\n",
        "append_body": "\n\nInvestigate immediately."
    },
    "rules": [                             // All rules must return true (AND logic). Empty [] = always fires
        {"rule": "exception_regex", "pattern": ".*database.*"},
        {"rule": "env_vars_matches", "env_var_name": "ENVIRONMENT", "env_var_values": ["production"]}
    ]
}
```

### Template Formatting

All 4 template fields (`prepend_title`, `append_title`, `prepend_body`, `append_body`) must be present in your configuration. Use empty strings `""` if you don't want formatting applied. Each trigger has its own template, allowing the same [channel](channels.md) to receive differently formatted messages from different triggers.

### Rules

Rules are conditions evaluated against exceptions. ALL rules in a trigger must return `true` for the trigger to fire (AND logic). For OR logic between conditions, create separate triggers (see [common patterns](README.md#common-patterns) for examples).

**Exception Regex Rule**

Matches exception message against regex pattern using `re.search()`. Returns `true` if pattern matches exception message, `false` otherwise. Empty pattern returns `true`. Test your regex patterns using the [validate command](README.md#testing-alert-configuration) with `--test-exception="your test message"`.

```jsonc
{"rule": "exception_regex", "pattern": ".*database.*|.*timeout.*|.*connection.*"}
```

**Environment Variable Match Rule**

Checks if environment variable has one of the expected values using `os.environ.get()`. Returns `true` if `os.environ[env_var_name]` equals any value in `env_var_values`, `false` otherwise. If env var doesn't exist, returns `false`. Empty `env_var_name` returns `true`.

```jsonc
{"rule": "env_vars_matches", "env_var_name": "ENVIRONMENT", "env_var_values": ["production", "prod"]}
```

**No Rules (Always Fire)**

Empty rules array means trigger fires for every exception. Use this for [audit logging patterns](README.md#common-patterns) where you want to capture all errors:

```jsonc
"rules": []
```