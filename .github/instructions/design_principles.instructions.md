---
applyTo: "**/*.py"
---
# Design Principles
Always apply all design principles, best practices, and patterns to your code. This document outlines the key principles, patterns, and practices to follow when writing code.

## Code Quality

### Naming and Readability
- **Meaningful Names**: Use descriptive names for variables, functions, and classes that clearly convey their purpose
- **Comments**: Explain *why* something is done, not what is done (code should be self-explanatory)
- **Avoid Magic Numbers**: Use named constants instead of hard-coded values

### Testing and Error Handling
- **Write Tests**: Use pytest for unit testing, contain tests in classes when logical, leverage unittest for mocking
- **Use Exceptions**: Prefer exceptions over error codes or print statements for error handling
- **Use Logging**: Employ the `logging` module instead of print statements for debugging and information

### Performance and Optimization
- **Avoid Premature Optimization**: Focus on correctness first, then optimize if necessary
- **Avoid Complex List Comprehensions**: Use regular loops for complex logic to maintain readability

## Core Principles

### Fundamental Design Principles
- **DRY (Don't Repeat Yourself)**: Abstract common functionality into reusable components
- **KISS (Keep It Simple, Stupid)**: Write simple, straightforward code and avoid unnecessary complexity
- **YAGNI (You Aren't Gonna Need It)**: Add functionality only when necessary to avoid over-engineering
- **Fail Fast**: Design code to fail quickly and clearly when errors occur for easier debugging

### SOLID Principles
- **Single Responsibility Principle**: A class should have only one reason to change
- **Open/Closed Principle**: Classes should be open for extension but closed for modification
- **Liskov Substitution Principle**: Subtypes must be substitutable for their base types
- **Interface Segregation Principle**: Clients should not depend on interfaces they don't use
- **Dependency Inversion Principle**: High-level modules should depend on abstractions, not low-level details

### GRASP Principles
- **Information Expert**: Assign responsibility to the class with the necessary information
## Behavioral Design Patterns (Gang of Four + More)
- **Observer**: Notify subscribers of state changes
- **Strategy**: Encapsulate interchangeable behavior
- **Command**: Encapsulate requests as objects
- **State**: Allow objects to change behavior based on internal state
- **Visitor**: Separate algorithm from object structure
- **Mediator**: Reduce direct communication between objects
- **Chain of Responsibility**: Pass requests along a chain of handlers

## Development Workflow Practices
- **Test-Driven Development (TDD)**: Write tests before writing code
- **Behavior-Driven Development (BDD)**: Focus on system behavior from the user's perspective
- **Refactoring**: Continuously improve the code structure without changing behavior

## Code Smell Anti-Patterns to Avoid
- **God Object / God Class**: A class that knows too much or does too much
- **Long Method**: Difficult to understand and test; break it up
- **Switch Statements**: Often a sign you should use polymorphism instead
- **Duplicated Code**: Refactor to remove repetition
- **Shotgun Surgery**: Making a small change requires many small changes to different classes
- **Feature Envy**: A method that accesses data of another object more than its own
- **Primitive Obsession**: Overusing basic types where rich domain types would help
- **Boolean Trap**: Using multiple boolean arguments, making function calls confusing
