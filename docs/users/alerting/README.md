# Alerting
The alerting mechanism and configuration is separated into two parts, channels and triggers. A channel is a configuration object to communicate with an external system like via email, PagerDuty, MS Teams or Slack, etc. A trigger references one or multiple channels and contains rules that all must evaluate to TRUE to send an alert to all referenced channels.

## How It Works
1. **Exception occurs** during pipeline execution
2. **System evaluates all enabled triggers** against the exception
3. **For each matching trigger**:
   - Apply template formatting to title and body
   - Send alert to all channels listed in `channel_names`

## Components
**Channels**: Destinations where alerts are sent (email, HTTP webhook, file). Define once, reference from multiple triggers.

**Triggers**: Conditional rules that determine when and where to send alerts. Contains:
- `enabled`: Whether this trigger is active
- `channel_names`: List of channel names to notify, must match exactly.
- `template`: Formatting for alert messages
- `rules`: Conditions that must ALL match (AND logic) for trigger to fire

**Rules**: Individual conditions evaluated against exceptions:
- `exception_regex`: Match exception message with regex pattern
- `env_vars_matches`: Match environment variable values
- Empty rules `[]`: Always fires for any exception

## Configuration
Root structure requires `alert` key with `channels` and `triggers` arrays:

```jsonc
{
    "alert": {
        "channels": [
            /* channel definitions */
        ],
        "triggers": [
            /* trigger definitions */
        ]
    }
}
```

## Channels
Channels define WHERE alerts are sent. Each requires unique `name` (for trigger references) and `channel_id` (type discriminator). Channels are only alerted via the trigger. A channel without trigger referencing it will never do anything.

## Testing
Test alert configuration without running full pipeline using the `validate` command:

```bash
python -m flint validate \
    --alert-filepath="examples/job.jsonc" \
    --runtime-filepath="examples/job.jsonc" \
    --test-exception="database connection failed" \
    --test-env-var=ENVIRONMENT=production \
    --test-env-var=APP_MODE=live
```
The test-exception will raise `FlintAlertTestError` with as message what was inputted. The test-env-vars creats environemtn variables on the system.
These may be used to test the alerting configuration to determine if it will actually trigger your rules. The above command should trigger alerting in the example configuration, although it will crash due to incorrect channel values.


## Common Patterns

**Environment-specific routing**: Route production errors to PagerDuty, dev errors to Slack.

```jsonc
{
    "triggers": [
        {
            "name": "prod-errors",
            "enabled": true,
            "description": "",
            "channel_names": ["pagerduty-oncall"],
            "template": {
                "prepend_title": "🚨 ","append_title": "",
                "prepend_body": "",
                "append_body": ""
            },
            "rules": [
                {"rule": "env_vars_matches",  "env_var_name": "ENVIRONMENT", "env_var_values": ["production", "acceptance"]}
            ]
        },
        {
            "name": "dev-errors",
            "enabled": true,
            "description": "",
            "channel_names": ["slack-dev"],
            "template": {
                "prepend_title": "DEV: ", "append_title": "",
                "prepend_body": "",
                "append_body": ""
            },
            "rules": [
                {"rule": "env_vars_matches", "env_var_name": "CLUSTER_TYPE", "env_var_values": ["GENERAL_PURPOSE"]}
            ]
        }
    ]
}
```

**Multiple triggers**: Route database errors to DBA team and network errors to NetOps team. Both may be alerted if both trigger rules evaluate to true.

```jsonc
{
    "triggers": [
        {
            "name": "db-errors",
            "enabled": true,
            "description": "",
            "channel_names": ["dba-team"],
            "template": {"prepend_title": "DB: ", "append_title": "", "prepend_body": "", "append_body": ""},
            "rules": [
                {"rule": "exception_regex", "pattern": ".*database.*|.*sql.*"}
            ]
        },
        {
            "name": "network-errors",
            "enabled": true,
            "description": "",
            "channel_names": ["netops-team"],
            "template": {"prepend_title": "NET: ", "append_title": "", "prepend_body": "", "append_body": ""},
            "rules": [
                {"rule": "exception_regex", "pattern": ".*network.*|.*connection.*|.*timeout.*"}
            ]
        }
    ]
}
```

**Audit trail catch-all**: Log all errors regardless of circumstance, exception or environment.

```jsonc
{
    "name": "audit-all",
    "enabled": true,
    "description": "",
    "channel_names": ["audit-log"],
    "template": {"prepend_title": "", "append_title": "", "prepend_body": "", "append_body": ""},
    "rules": []
}
```


## FAQ

**Q: Can I reuse alert config across multiple pipelines?**  
A: Yes. Specify same alert file for multiple runs: `python -m flint run --alert-filepath="alerts.jsonc" --runtime-filepath="pipeline1.json"`

**Q: How do I implement OR logic between rules?**  
A: Create separate triggers. Within a trigger, rules use AND logic. Multiple triggers provide OR behavior.

**Q: What happens if a channel fails to send?**  
A: Depends on `retry.raise_on_error`. If `false` (default), error is logged and pipeline continues. If `true`, pipeline fails after exhausting retry attempts.

**Q: Can one trigger send to multiple channels?**  
A: Yes. List multiple channel names: `"channel_names": ["email", "slack", "audit"]`

**Q: Are alerts sent on successful pipeline runs?**  
A: No. Alerts only fire on exceptions. Use runtime config event hooks (`onSuccess`) for success notifications.

**Q: Do I need to define all 4 template fields?**  
A: Yes. All 4 fields (`prepend_title`, `append_title`, `prepend_body`, `append_body`) are required. Use empty strings `""` if you don't want that formatting.

**Q: Can I disable a channel temporarily?**  
A: No `enabled` field exists on channels. Disable the trigger instead using `"enabled": false`, or remove the channel name from trigger's `channel_names`.
