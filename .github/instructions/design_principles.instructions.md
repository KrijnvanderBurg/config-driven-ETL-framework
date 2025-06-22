---
applyTo: "**/*.py"
---
# Design Principles and Best Practices for software development
- DRY (Don't Repeat Yourself): Avoid code duplication by abstracting common functionality into reusable functions or classes.
- KISS (Keep It Simple, Stupid): Write simple and straightforward code. Avoid unnecessary complexity.
- YAGNI (You Aren't Gonna Need It): Don't add functionality until it is necessary. Avoid over-engineering.
- SOLID principles: Follow the SOLID principles of object-oriented design to create maintainable and scalable code.
  - Single Responsibility Principle: A class should have one reason to change.
  - Open/Closed Principle: Classes should be open for extension but closed for modification.
  - Liskov Substitution Principle: Subtypes must be substitutable for their base types without altering the correctness of the program.
  - Interface Segregation Principle: Clients should not be forced to depend on interfaces they do not use.
  - Dependency Inversion Principle: High-level modules should not depend on low-level modules; both should depend on abstractions.
- GRASP:
  - Information Expert: Assign responsibility to the class that has the necessary information to fulfill it.
  - Creator: A class should be responsible for creating instances of classes that it uses.
  - Controller: A class should delegate user input to appropriate classes.
  - Low Coupling: Classes should be as independent as possible to reduce dependencies.
  - High Cohesion: Classes should have a well-defined purpose and related functionality.
- Fail Fast: Write code that fails quickly and clearly when an error occurs, making it easier to debug.
- Law of Demeter: A method should only call methods on:
  - Itself
  - Its parameters
  - Any objects it creates
  - Any objects it holds references to
- Avoid premature optimization: Focus on writing clear and maintainable code first, then optimize if necessary
- Use comments judiciously: Write comments to explain why something is done, not what is done. The code itself should be clear enough to explain what it does.
- Write tests: Always write unit tests for your code. Use pytest primarily, contain tests in a class if possible, use unittest framework for mocking.
- Tell, don't ask: Use polymorphism instead of conditionals to handle different behaviors based on types.
- Encapsulation: Hide internal state and require all interaction through methods.
- Avoid list comprehensions for complex logic.
- Separation of concerns: Keep different aspects of your code (e.g., business logic, data access, presentation) in separate modules or classes.
- Immutability: Prefer immutable data structures where possible to avoid unintended side effects.
- Separation of Concerns	Divide code into distinct sections that each address a separate concern.
- Composition over Inheritance	Favor assembling behavior with components over rigid class hierarchies.
- Convention over Configuration	Use sensible defaults and limit the need for extensive setup.
- Use Meaningful Names	Use descriptive names for variables, functions, classes.
- Limit Function Size	Keep functions small and focused; ideally one task.
- Code to Interface, Not Implementation	Use abstractions for flexibility and testability.
- Avoid Premature Optimization	First make it work, then make it fast only if needed.
- Minimize Side Effects	Functions should do one thing and avoid modifying global state.
- Single Level of Abstraction per Function	Don’t mix low-level details with high-level logic.
- Postel's Law: Be liberal in what you accept, and conservative in what you send.
- Principle of Least Privilege	Code should run with the minimal level of access required.
- Keep functions short: Aim for functions that are no longer than 20-30 lines.
- Avoid side effects: Functions should not modify global state or have hidden effects.
- Use exceptions for error handling: Prefer raising exceptions over returning error codes or using print statements.
- Use logging for debugging and information: Use the `logging` module instead of print statements for debug output.
- Avoid code duplication: If you find yourself copying and pasting code, consider refactoring it into a separate function.
- Avoid deep nesting: apply early Returns to reduce the complexity.
- Use meaningful names: Choose variable and function names that clearly convey their purpose.
- Avoid magic numbers: Use named constants instead of hard-coded numbers.

## Behavioral Design Patterns (Gang of Four + More)
- Observer	Notify subscribers of state changes.
- Strategy	Encapsulate interchangeable behavior.
- Command	Encapsulate requests as objects.
- State	Allow objects to change behavior based on internal state.
- Visitor	Separate algorithm from object structure.
- Mediator	Reduce direct communication between objects.
- Chain of Responsibility	Pass requests along a chain of handlers.

## Development Workflow Practices
- Test-Driven Development (TDD)	Write tests before writing code.
- Behavior-Driven Development (BDD)	Focus on system behavior from the user’s perspective.
- Refactoring	Continuously improve the code structure without changing behavior.

# Code Smell Anti-Patterns to Avoid
- God Object / God Class	A class that knows too much or does too much.
- Long Method	Difficult to understand and test. Break it up.
- Switch Statements	Often a sign you should use polymorphism instead.
- Magic Numbers	Use named constants instead of literal values.
- Duplicated Code	Refactor to remove repetition.
- Shotgun Surgery	Making a small change requires many small changes to different classes.
- Feature Envy	A method that accesses data of another object more than its own.
- Primitive Obsession	Overusing basic types where rich domain types would help.
- Boolean Trap	Using multiple boolean arguments, making function calls confusing.
