

### Email Channel

Sends alerts via SMTP. Uses email subject for title, body for message content.

**Implementation**: Creates `MIMEText` message, authenticates with SMTP server using provided credentials, sends to all addresses in `to_emails`.

**Required fields**:
- `channel_id`: Must be literal `"email"`
- `name`: Unique identifier for this channel (used in trigger `channel_names`)
- `smtp_server`: SMTP server hostname/IP
- `smtp_port`: SMTP port (587 for STARTTLS, 465 for SSL/TLS)
- `username`: SMTP authentication username
- `password`: SMTP authentication password (SecretStr type)
- `from_email`: Sender address (validated as email)
- `to_emails`: Array of recipient addresses (minimum 1, validated as emails)

**Optional fields**:
- `description`: Documentation string (default: `""`)

```jsonc
{
    "channel_id": "email",
    "name": "ops-email",
    "description": "Production operations team alerts",
    "smtp_server": "smtp.company.com",
    "smtp_port": 587,
    "username": "alerts@company.com",
    "password": "secret",
    "from_email": "alerts@company.com",
    "to_emails": ["ops@company.com", "oncall@company.com"]
}
```