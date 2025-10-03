### File Channel

Appends alerts to local filesystem file. Creates file if doesn't exist.

**Implementation**: Opens file in append mode with UTF-8 encoding, writes formatted line: `{title}: {body}\n`.

**Required fields**:
- `channel_id`: Must be literal `"file"`
- `name`: Unique identifier (used in trigger `channel_names`)
- `file_path`: Path to log file (can be relative or absolute)

**Optional fields**:
- `description`: Documentation string (default: `""`)

```jsonc
{
    "channel_id": "file",
    "name": "audit-log",
    "description": "Audit trail of all pipeline errors",
    "file_path": "/var/log/flint/alerts.log"
}
```