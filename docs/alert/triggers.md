

# Alert Triggers

Configuration reference for triggers and rules.

## Trigger Configuration

```jsonc
{
    "id": "prod-critical",                      // Must be unique within configuration
    "description": "Production critical errors route to ops team",
    "enabled": true,                              // Boolean to activate/deactivate
    "channel_ids": ["ops-email", "slack-webhook"],  // Must match channel name fields exactly
    "template": {
        "prepend_title": "🚨 PROD: ",            // Text added before exception title
        "append_title": "",                       // Text added after exception title (can be empty)
        "prepend_body": "Critical error detected:\n",  // Text added before exception message
        "append_body": "\n\nInvestigate immediately."  // Text added after exception message
    },
    "rules": [                                    // All rules must return true (AND logic). Empty [] = always fires
        {"rule_type": "exception_regex", "pattern": ".*database.*"},
        {"rule_type": "env_vars_matches", "env_var_name": "ENVIRONMENT", "env_var_values": ["production"]}
    ]
}
```

## Rules

Rules determine whether a trigger fires based on exception properties or environment conditions. All rules within a trigger use AND logic.

### Exception Regex Rule

Matches the exception message against a regular expression pattern.

```jsonc
{
    "rule_type": "exception_regex", "pattern": ".*database.*|.*timeout.*|.*connection.*"
}
```

Uses Python's `re.search()` for matching. Empty pattern `""` always matches.

### Environment Variable Match Rule

Matches environment variable values against expected values.

```jsonc
{
    "rule_type": "env_vars_matches", "env_var_name": "ENVIRONMENT", "env_var_values": ["production", "prod"]
}
```

Returns `false` if the environment variable doesn't exist. Empty `env_var_name` always matches.

### No Rules (Always Fire)

Empty rules array fires for every exception:

```jsonc
"rules": []  // Trigger fires for all exceptions
```
