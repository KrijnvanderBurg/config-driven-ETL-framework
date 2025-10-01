---
applyTo: "**/*.py"
---
# Python Coding Principles

## Type Annotations
- Use type hints for all code (functions, methods, variables, return values)
- Prefer Python built-in types over typing module: `dict` > `Dict`, `list` > `List`
- Use pipe operator for unions: `str | None` > `Optional[str]`
- Use `None` as default for optional parameters, not empty collections

## String Formatting
- Use f-strings for string interpolation: `f"Hello {name}"`
- Use lazy formatting for logs: `logger.debug("Processing %s items", count)`
- use f-strings for exceptions messages: `raise ValueError(f"Invalid value: {value}")`
- Format numbers with precision in f-strings: `f"Value: {value:.2f}"`

## Docstrings
- Follow Google-style docstring format
- Include brief description of functionality
- Document all parameters in Args section
- Document return values in Returns section
- Document exceptions in Raises section when applicable
- Provide usage examples for public methods
- Use triple double-quotes `"""` for all docstrings

## Naming Conventions
- Classes: PascalCase (`MyClass`)
- Functions/methods: snake_case (`process_data`)
- Variables: snake_case (`user_count`)
- Constants: UPPER_SNAKE_CASE (`MAX_CONNECTIONS`)
- Private methods/attributes: _leading_underscore (`_internal_method`)
- Protected (intended for subclasses): _leading_underscore
- Double leading underscore (`__method`) only for name mangling

## Best Practices
- Use context managers with `with` for resource management
- Use generators and comprehensions for efficient data processing
- Use dataclasses or named tuples for data containers
- Avoid global variables and mutable default arguments
- Use enumeration for related constants (`from enum import Enum`)
- Handle exceptions at appropriate levels, use built-in exceptions.

## Design patterns and principles
Focus on maintaining clean architecture, readability, and clean code principles. Always adhere to design patterns and principles like:
- Single Responsibility Principle (SRP)
- Open/Closed Principle (OCP)
- Dependency Inversion
- Don't Repeat Yourself (DRY)
- SOLID principles
- Composition over Inheritance
- Separation of concerns

## Don´t
- Do not use `print`, use logging
- Do not use `isinstance`, `hasattr`, or `getattr`.
- Do not use `eval` or `exec` for dynamic code execution
- Do not use `from module import *`, import explicitly
