
### HTTP Channel

Sends alerts to HTTP endpoints as JSON POST requests. Automatically formats payload as `{"title": "...", "message": "..."}`.

**Implementation**: Inherits from `HttpBase`, makes HTTP request with configured method/headers/timeout, implements retry logic with exponential backoff.

**Required fields**:
- `channel_id`: Must be literal `"http"`
- `name`: Unique identifier (used in trigger `channel_names`)
- `url`: HTTP endpoint URL
- `method`: HTTP method (e.g., "POST", "PUT")
- `headers`: Dictionary of HTTP headers

**Optional fields**:
- `description`: Documentation string (default: `""`)
- `timeout`: Request timeout in seconds (default: 30)
- `retry`: Retry configuration object:
  - `raise_on_error`: If `true`, fail pipeline on alert failure; if `false`, log and continue (default: `false`)
  - `max_attempts`: Maximum retry attempts (default: 3)
  - `delay_in_seconds`: Delay between retries (default: 30)

```jsonc
{
    "channel_id": "http",
    "name": "slack-webhook",
    "description": "Engineering team Slack channel",
    "url": "https://hooks.slack.com/services/T00/B00/XX",
    "method": "POST",
    "headers": {"Content-Type": "application/json"},
    "timeout": 30,
    "retry": {
        "raise_on_error": false,
        "max_attempts": 3,
        "delay_in_seconds": 30
    }
}
```