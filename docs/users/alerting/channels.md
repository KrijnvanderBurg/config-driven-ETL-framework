# Alert Channels

Channels define the destinations where alerts are sent. Each channel represents a specific communication method (email, HTTP webhook, file) configured with connection details and credentials.

## Overview

Channels are notification endpoints. Define a channel once in the `channels` array with a unique `name` identifier. Channels are activated by triggers (see [trigger documentation](triggers.md) for how to route alerts to channels).

**Key characteristics**:
- Each channel has a unique `name` identifier
- All configuration fields must be present (use empty strings `""` for unused fields)
- Channels don't have an `enabled` field

## Configuration Structure

All channels share this base structure within the root [alert configuration](README.md#configuration):

```jsonc
{
    "alert": {
        "channels": [
            {
                "channel_id": "email|http|file",  // Channel type discriminator
                "name": "unique-channel-name",     // Unique identifier
                "enabled": true,
                "description": "",                 // Documentation (can be empty)
                // ... channel-specific fields
            }
        ]
    }
}
```


## Email Channel

Sends alerts via SMTP. Uses email subject for title, body for message content. Creates `MIMEText` message, authenticates with SMTP server, sends to all addresses in `to_emails`.

```jsonc
{
    "channel_id": "email",
    "name": "ops-email",
    "description": "Production operations team alerts",
    "smtp_server": "smtp.company.com",
    "smtp_port": 587,                      // 587 for STARTTLS, 465 for SSL/TLS
    "username": "alerts@company.com",
    "password": "secret",                  // Use environment variables for sensitive data
    "from_email": "alerts@company.com",
    "to_emails": ["ops@company.com", "oncall@company.com"]
}
```

---

## HTTP Channel

Sends alerts to HTTP endpoints as JSON POST requests. Automatically formats payload as `{"title": "...", "message": "..."}`. Implements retry logic with exponential backoff.

```jsonc
{
    "channel_id": "http",
    "name": "slack-webhook",
    "description": "Engineering team Slack channel",
    "url": "https://hooks.slack.com/services/T00/B00/XX",
    "method": "POST",                      // POST, PUT, etc.
    "headers": {"Content-Type": "application/json"},
    "timeout": 30,                         // Request timeout in seconds
    "retry": {
        "raise_on_error": false,           // true: fail pipeline on alert failure, false: log and continue
        "max_attempts": 3,
        "delay_in_seconds": 30
    }
}
```

**Use cases**: Slack, PagerDuty, Microsoft Teams, custom webhooks, internal APIs.

---

## File Channel

Appends alerts to local filesystem file. Creates file if doesn't exist. Opens file in append mode with UTF-8 encoding, writes: `{title}: {body}\n`.

```jsonc
{
    "channel_id": "file",
    "name": "audit-log",
    "description": "Audit trail of all pipeline errors",
    "file_path": "/var/log/flint/alerts.log"  // Can be relative or absolute
}
```

## Channel Examples

### Multiple Channels Configuration

```jsonc
{
    "channels": [
        {
            "channel_id": "email",
            "name": "ops-email",
            "description": "",
            "smtp_server": "smtp.company.com",
            "smtp_port": 587,
            "username": "alerts@company.com",
            "password": "secret",
            "from_email": "alerts@company.com",
            "to_emails": ["ops@company.com"]
        },
        {
            "channel_id": "http",
            "name": "pagerduty",
            "description": "",
            "url": "https://events.pagerduty.com/v2/enqueue",
            "method": "POST",
            "headers": {"Content-Type": "application/json"},
            "timeout": 30,
            "retry": {"raise_on_error": false, "max_attempts": 3, "delay_in_seconds": 30}
        },
        {
            "channel_id": "file",
            "name": "audit",
            "description": "",
            "file_path": "/var/log/alerts.log"
        }
    ]
}
```