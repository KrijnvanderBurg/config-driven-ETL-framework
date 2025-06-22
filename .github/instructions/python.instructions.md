---
applyTo: "**/*.py"
---
# Python principles

- Type hints for everything: use python built-in types over typing module. dict > Dict, list > List, | > Optional.
- Use lazy formatting for logs.
- Use f-strings for string formatting.

Docstrings:
- Google-style
- Brief description
- Args section
- Returns section
- Raises section if applicable
- Example if the method is public.

Naming conventions:
- Classes: PascalCase
- Functions/methods: snake_case
- Variables: snake_case
- Constants: UPPER_SNAKE_CASE
- Private methods/attributes: _leading_underscore
