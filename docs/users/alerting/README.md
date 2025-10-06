# Alerting
The alerting system enables configuration-driven notifications when errors occur. It consists of two parts: [channels](./channels.md), the destination where to send alerts to; and [triggers](./triggers.md), conditional and routing logic when to send alerts to a channel.

## How It Works
1. An error occurs.
2. All triggers are evaluated
    - Trigger is enabled.
    - All configured rules return `true`.
3. Template formatting is applied to alert title and body
4. Alert is send to all referenced channels.

## Channels
[Channels](channels.md) are reusable notification endpoints. Define a channel once, then reference it from any [trigger](triggers.md). Multiple triggers can send to the same channel, and one trigger can send to multiple channels simultaneously. Each trigger has its own alert template, so the same channel can receive differently formatted messages from different triggers.

**An example channel for Email**:

```jsonc
{
    "alert": {
        "channels": [
            {
                "id": "ops-email",
                "description": "Production operations team alerts",
                "enabled": true,
                "channel_type": "email",
                // Email channel specific configuration.
                "smtp_server": "smtp.company.com",
                "smtp_port": 587,
                "username": "username",
                "password": "password",
                "from_email": "alerts@company.com",
                "to_emails": ["ops@company.com", "oncall@company.com"]
            },
            // ... Add more channels
        ],
        "triggers": [
            /* Trigger definitions */
            // A channel without trigger does nothing.
        ]
    }
}
```

**Available channels**:
- **[Email](channels.md#email-channel)**: Send via SMTP to email recipients.
- **[HTTP](channels.md#http-channel)**: Send a HTTP request to webhooks (Slack, PagerDuty, Teams, custom APIs).
- **[File](channels.md#file-channel)**: Write and append to local filesystem file.

See [channels documentation](channels.md) for complete configuration details and examples.

## Triggers
[Triggers](triggers.md) contain conditional logic that determines when to send alerts and to which channel. Within a single trigger, all [rules](triggers.md#rules) use AND logic — every rule must return `true` for the trigger to send an alert.

Multiple triggers can fire for the same exception, each routing to different channels with different message formatting.

**Available rules**:
- **Exception Regex**: ...
- **Environment variable matches**: ...

See [triggers documentation](triggers.md) for complete configuration details and examples.


**An example trigger**:

```jsonc
{
    "alert": {
        "channels": [
            {
                "id": "network-ops-email",
                "description": "Production operations team alerts",
                "enabled": true,
                "channel_type": "email",
                // Email channel specific configuration.
                "smtp_server": "smtp.company.com",
                "smtp_port": 587,
                "username": "username",
                "password": "password",
                "from_email": "alerts@company.com",
                "to_emails": ["ops@company.com", "oncall@company.com"]
            },
            // ... Add more channels
        ],
        "triggers": [
            {
                "id": "dev-errors",
                "description": "Network errors to network-ops-email",
                "enabled": true,
                "channel_ids": ["network-ops-email"],
                "template": {
                    "prepend_title": "DEV: ",
                    "append_title": "",
                    "prepend_body": "",
                    "append_body": ""
                },
                "rules": [
                    {"rule": "env_vars_matches", "env_var_name": "ENVIRONMENT", "env_var_values": ["acceptance", "production"]},
                    {"rule": "exception_regex", "pattern": ".*network.*|.*connection.*|.*timeout.*"}
                ]
            },
            // ... Add more triggers 
        ]
    }
}
```

## Configuration

The following configuration structure is required. The root `alert` key with channels and triggers subkeys must always be present. The channels and triggers may, however, be empty lists. Meaning ofcourse that no alert is being send. 

```jsonc
{
    "alert": {
        "channels": [/* channel definitions */],
        "triggers": [/* trigger definitions */]
    }
}
```

A channel always requires a trigger to work. A channel without trigger is never send. 

The same alert configuration file can be reused across multiple pipelines by specifying it in different runs:

Because the alert and runtime are supplied separately you can separate the alert from the various jobs, this allows you to define one alert configuration for one or many jobs. 

```bash
python -m flint run \
    --alert-filepath="alerts.jsonc" \
    --runtime-filepath="job.jsonc"`
```

### Testing
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

## Complete example of use cases

Route production errors to PagerDuty, development errors to Slack using [env_vars_matches rules](triggers.md#rules):

Route different error types to specialized teams using [exception_regex rules](triggers.md#rules). Both triggers may fire for the same exception if rules match:

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
            "retry": {
                "max_attempts": 3,
                "delay_in_seconds": 30
            }
        },
        {
            "id": "ops-email",
            "description": "Production operations team alerts",
            "enabled": true,
            "channel_type": "email",
            // Email channel specific configuration.
            "smtp_server": "smtp.company.com",
            "smtp_port": 587,
            "username": "username",
            "password": "password",
            "from_email": "alerts@company.com",
            "to_emails": ["ops@company.com", "oncall@company.com"]
        },
        {
            "channel_id": "http",
            "name": "slack-dev",
            "description": "",
            "url": "https://hooks.slack.com/services/DEV/WEBHOOK/URL",
            "method": "POST",
            "headers": {"Content-Type": "application/json"},
            "timeout": 30,
            "retry": {
                "max_attempts": 3,
                "delay_in_seconds": 30
            }
        },
        
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
                {"rule": "env_vars_matches", "env_var_name": "ENVIRONMENT", "env_var_values": ["development", "dev"]},
                {"rule": "exception_regex", "pattern": ".*network.*|.*connection.*|.*timeout.*"}
            ]
        },
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


## Best Practices

**Required fields**: All configuration fields must be present—use empty strings `""` for unused fields. This ensures configuration consistency and explicit intent across all [channels](channels.md) and [triggers](triggers.md).

**Test before production**: Use the [`validate` command](#testing-alert-configuration) to test configurations before deploying to ensure [trigger rules](triggers.md#rules) match expected patterns and [channel connections](channels.md#testing-and-validation) work correctly.

**Documentation**: Fill in `description` fields to explain the purpose of each [channel](channels.md) and [trigger](triggers.md) for team clarity.
