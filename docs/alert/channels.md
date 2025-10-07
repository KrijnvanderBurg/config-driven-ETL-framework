# Alert Channels

Configuration reference for available alert channel types.

## Configuration Structure

All channels share this base structure:

```jsonc
{
    "id": "unique-channel-id",          // Unique identifier for reference in triggers
    "description": "",                // Documentation (optional but recommended)
    "enabled": true,
    "channel_type": "email|http|file",  // Channel type identifier
    // ... channel-specific fields
}
```
## Email Channel

Sends alerts via SMTP email.

```jsonc
{
    "id": "ops-email",
    "description": "Production operations team alerts",
    "enabled": true,
    "channel_type": "email",
    // email specific fields
    "smtp_server": "smtp.company.com",
    "smtp_port": 587,                      // 587 for STARTTLS, 465 for SSL/TLS
    "username": "alerts@company.com",
    "password": "secret",                  // Use environment variables for sensitive data
    "from_email": "alerts@company.com",
    "to_emails": ["ops@company.com", "oncall@company.com"]
}
```

## HTTP Channel

Sends alerts via HTTP requests to webhooks and API endpoints.

```jsonc
{
    "id": "slack-webhook",
    "description": "Engineering team Slack channel",
    "enabled": true,
    "channel_type": "http",
    // http specific fields
    "url": "https://hooks.slack.com/services/T00/B00/XX",
    "method": "POST",                      // POST | GET
    "headers": {
        "Content-Type": "application/json"
    },
    "timeout": 30,                         // Request timeout in seconds
    "retry": {
        "max_attempts": 3,
        "delay_in_seconds": 30
    }
}
```

## File Channel

Appends alerts to a local file for logging and auditing.

```jsonc
{
    "id": "audit-log",
    "description": "Audit trail of all pipeline errors",
    "enabled": true,
    "channel_type": "file",
    // file specific fields
    "file_path": "/var/log/flint/alerts.log"  // Can be relative or absolute
}
```
