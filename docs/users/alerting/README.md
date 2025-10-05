# Alerting

The alerting system enables configuration-driven notifications when pipeline exceptions occur. Configure notification destinations ([channels](channels.md)) and conditional logic ([triggers](triggers.md) with rules) to route alerts to appropriate teams based on exception type and environment context.

## Overview

Alerting configuration consists of two components working together:

1. **[Channels](channels.md)**: Define WHERE alerts are sent (email, HTTP webhooks, file logs)
2. **[Triggers](triggers.md)**: Define WHEN and TO WHICH channels alerts are sent (using conditional rules)

All fields must always be present in your configuration file. Fields like `description` can be empty strings `""`, but cannot be omitted entirely.

## How It Works

When a pipeline exception occurs:

1. **Exception is caught** by the Flint runtime
2. **All enabled [triggers](triggers.md) are evaluated** against the exception and current environment
3. **For each trigger where ALL rules match**:
   - [Template formatting](triggers.md#template-formatting) is applied to the alert title and body
   - Alert is sent to all [channels](channels.md) listed in the trigger's `channel_names`
4. **Pipeline execution** continues or fails based on [channel retry settings](channels.md#error-handling)

## Channels

[Channels](channels.md) are reusable notification endpoints. Define a channel once, then reference it from any [trigger](triggers.md) using its `name` field. Multiple triggers can send to the same channel, and one trigger can send to multiple channels simultaneously by listing them in `channel_names`. Each trigger has its own template, so the same channel can receive differently formatted messages from different triggers.

**Available channel types**:
- **[Email](channels.md#email-channel)**: Send via SMTP to email recipients
- **[HTTP](channels.md#http-channel)**: POST JSON to webhooks (Slack, PagerDuty, Teams, custom APIs)
- **[File](channels.md#file-channel)**: Append to local filesystem log files

See [channels documentation](channels.md) for complete configuration details and examples.

## Triggers

[Triggers](triggers.md) contain conditional logic that determines when and where to send alerts. Within a single trigger, all [rules](triggers.md#rules) use AND logic—every rule must return `true` for the trigger to fire. For OR logic between conditions, create separate triggers. This means multiple triggers can fire for the same exception, each routing to different channels with different message formatting.

**Key concepts**:
- `enabled`: Boolean to activate/deactivate the trigger
- `channel_names`: Array of [channel names](channels.md#overview) to notify (must match exactly)
- `template`: All 4 formatting fields required (use `""` for no formatting)
- `rules`: Conditions evaluated against exceptions ([exception_regex](triggers.md#rules), [env_vars_matches](triggers.md#rules), or empty `[]` for catch-all)

See [triggers documentation](triggers.md) for complete configuration details and examples.

## Configuration

Root structure requires `alert` key with [channels](channels.md) and [triggers](triggers.md) arrays. The same alert configuration file can be reused across multiple pipelines by specifying it in different runs: `python -m flint run --alert-filepath="alerts.jsonc" --runtime-filepath="pipeline1.jsonc"`.

```jsonc
{
    "alert": {
        "channels": [/* channel definitions */],
        "triggers": [/* trigger definitions */]
    }
}
```

## Quick Start Example

Complete working configuration with email and HTTP channels:

```jsonc
{
    "alert": {
        "channels": [
            {
                "channel_id": "email",
                "name": "ops-team",
                "description": "Operations team email",
                "smtp_server": "smtp.company.com",
                "smtp_port": 587,
                "username": "alerts@company.com",
                "password": "secret",
                "from_email": "alerts@company.com",
                "to_emails": ["ops@company.com"]
            },
            {
                "channel_id": "http",
                "name": "slack-alerts",
                "description": "Engineering Slack channel",
                "url": "https://hooks.slack.com/services/YOUR/WEBHOOK/URL",
                "method": "POST",
                "headers": {"Content-Type": "application/json"},
                "timeout": 30,
                "retry": {
                    "raise_on_error": false,
                    "max_attempts": 3,
                    "delay_in_seconds": 30
                }
            },
            {
                "channel_id": "file",
                "name": "audit-log",
                "description": "Local audit trail",
                "file_path": "/var/log/flint/alerts.log"
            }
        ],
        "triggers": [
            {
                "name": "production-errors",
                "enabled": true,
                "description": "Alert ops team for production issues",
                "channel_names": ["ops-team", "slack-alerts", "audit-log"],
                "template": {
                    "prepend_title": "🚨 PRODUCTION: ",
                    "append_title": "",
                    "prepend_body": "Environment: Production\n\n",
                    "append_body": "\n\nImmediate action required."
                },
                "rules": [
                    {"rule": "env_vars_matches", "env_var_name": "ENVIRONMENT", "env_var_values": ["production"]}
                ]
            },
            {
                "name": "all-errors-audit",
                "enabled": true,
                "description": "Log all errors for audit trail",
                "channel_names": ["audit-log"],
                "template": {
                    "prepend_title": "",
                    "append_title": "",
                    "prepend_body": "",
                    "append_body": ""
                },
                "rules": []
            }
        ]
    }
}
```

## Testing Alert Configuration

Test your alert configuration without running a full pipeline using the `validate` command. This is useful for validating configuration syntax, testing [trigger rules](triggers.md#rules) match expected exceptions, verifying [channel connection](channels.md#testing-and-validation) details, and ensuring environment-based routing works correctly.

```bash
python -m flint validate \
    --alert-filepath="examples/job.jsonc" \
    --runtime-filepath="examples/job.jsonc" \
    --test-exception="database connection failed" \
    --test-env-var=ENVIRONMENT=production \
    --test-env-var=APP_MODE=live
```

**How it works**:
- `--test-exception`: Raises a `FlintAlertTestError` with the specified message for testing [regex patterns](triggers.md#rules)
- `--test-env-var`: Creates environment variables on the system (format: `KEY=VALUE`) for testing [env_vars_matches rules](triggers.md#rules)
- The command evaluates all [triggers](triggers.md) against the test exception
- Matching triggers attempt to send alerts through their configured [channels](channels.md)

## Common Patterns

### Environment-Specific Routing

Route production errors to PagerDuty, development errors to Slack using [env_vars_matches rules](triggers.md#rules):

```jsonc
{
    "channels": [
        {
            "channel_id": "http",
            "name": "pagerduty-oncall",
            "description": "",
            "url": "https://events.pagerduty.com/v2/enqueue",
            "method": "POST",
            "headers": {"Content-Type": "application/json"},
            "timeout": 30,
            "retry": {"raise_on_error": true, "max_attempts": 3, "delay_in_seconds": 30}  // Fail pipeline if critical alert fails
        },
        {
            "channel_id": "http",
            "name": "slack-dev",
            "description": "",
            "url": "https://hooks.slack.com/services/DEV/WEBHOOK/URL",
            "method": "POST",
            "headers": {"Content-Type": "application/json"},
            "timeout": 30,
            "retry": {"raise_on_error": false, "max_attempts": 3, "delay_in_seconds": 30}  // Continue pipeline if dev alert fails
        }
    ],
    "triggers": [
        {
            "name": "prod-errors",
            "enabled": true,
            "description": "Production errors to PagerDuty",
            "channel_names": ["pagerduty-oncall"],
            "template": {
                "prepend_title": "🚨 PRODUCTION: ",
                "append_title": "",
                "prepend_body": "",
                "append_body": ""
            },
            "rules": [
                {"rule": "env_vars_matches", "env_var_name": "ENVIRONMENT", "env_var_values": ["production", "prod"]}
            ]
        },
        {
            "name": "dev-errors",
            "enabled": true,
            "description": "Development errors to Slack",
            "channel_names": ["slack-dev"],
            "template": {
                "prepend_title": "DEV: ",
                "append_title": "",
                "prepend_body": "",
                "append_body": ""
            },
            "rules": [
                {"rule": "env_vars_matches", "env_var_name": "ENVIRONMENT", "env_var_values": ["development", "dev"]}
            ]
        }
    ]
}
```

### Exception-Based Routing

Route different error types to specialized teams using [exception_regex rules](triggers.md#rules). Both triggers may fire for the same exception if rules match:

```jsonc
{
    "triggers": [
        {
            "name": "database-errors",
            "enabled": true,
            "description": "Database issues to DBA team",
            "channel_names": ["dba-email"],
            "template": {
                "prepend_title": "DB: ",
                "append_title": "",
                "prepend_body": "",
                "append_body": ""
            },
            "rules": [
                {"rule": "exception_regex", "pattern": ".*database.*|.*sql.*|.*connection pool.*"}
            ]
        },
        {
            "name": "network-errors",
            "enabled": true,
            "description": "Network issues to NetOps team",
            "channel_names": ["netops-slack"],
            "template": {
                "prepend_title": "NETWORK: ",
                "append_title": "",
                "prepend_body": "",
                "append_body": ""
            },
            "rules": [
                {"rule": "exception_regex", "pattern": ".*network.*|.*connection.*|.*timeout.*"}
            ]
        }
    ]
}
```

### Audit Trail Catch-All

Log every error regardless of type or environment using empty [rules array](triggers.md#rules):

```jsonc
{
    "triggers": [
        {
            "name": "audit-all-errors",
            "enabled": true,
            "description": "Comprehensive audit trail",
            "channel_names": ["audit-log"],  // File channel for persistent logging
            "template": {
                "prepend_title": "",
                "append_title": "",
                "prepend_body": "",
                "append_body": ""
            },
            "rules": []  // Empty rules = fires for every exception
        }
    ]
}
```

### Multi-Channel Critical Alerts

Send critical production errors to multiple [channels](channels.md) for redundancy. All [channel names](channels.md#overview) must match exactly:

```jsonc
{
    "triggers": [
        {
            "name": "critical-prod-errors",
            "enabled": true,
            "description": "Critical production errors to all channels",
            "channel_names": ["pagerduty-oncall", "ops-email", "slack-critical", "audit-log"],
            "template": {
                "prepend_title": "🚨🚨🚨 CRITICAL: ",
                "append_title": "",
                "prepend_body": "CRITICAL PRODUCTION ERROR\n\n",
                "append_body": "\n\nImmediate escalation required."
            },
            "rules": [
                {"rule": "env_vars_matches", "env_var_name": "ENVIRONMENT", "env_var_values": ["production"]},
                {"rule": "exception_regex", "pattern": ".*critical.*|.*fatal.*|.*emergency.*"}
            ]
        }
    ]
}
```
## Best Practices

**Required fields**: All configuration fields must be present—use empty strings `""` for unused fields. This ensures configuration consistency and explicit intent across all [channels](channels.md) and [triggers](triggers.md).

**Credential security**: Never commit passwords or tokens to version control. Store sensitive values in environment variables and reference them from your deployment system. See [channel security best practices](channels.md#best-practices).

**Audit logging**: Always include a [file channel](channels.md#file-channel) with a catch-all [trigger](triggers.md) (`rules: []`) for comprehensive audit trails of all exceptions.

**Test before production**: Use the [`validate` command](#testing-alert-configuration) to test configurations before deploying to ensure [trigger rules](triggers.md#rules) match expected patterns and [channel connections](channels.md#testing-and-validation) work correctly.

**Unique naming**: Use descriptive names like `slack-prod-critical` instead of `channel1` for both [channels](channels.md#overview) and [triggers](triggers.md). Trigger names must be unique within a configuration file.

**Documentation**: Fill in `description` fields to explain the purpose of each [channel](channels.md) and [trigger](triggers.md) for team clarity.

**Redundancy**: Send critical production alerts to multiple [channels](channels.md#multi-channel-alerts) for reliability.

**Environment separation**: Use different [channels](channels.md#environment-specific-channels) for different environments (dev, staging, prod) with environment-based [trigger routing](triggers.md#rules).

---

## Working Example

See [`examples/job.jsonc`](../../../examples/job.jsonc) for a complete working configuration demonstrating [channels](channels.md), [triggers](triggers.md), and [rules](triggers.md#rules) in action.
