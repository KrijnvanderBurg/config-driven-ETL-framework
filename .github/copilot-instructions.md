Flint is a configuration-driven PySpark ETL framework. It uses JSON files to define extract-transform-load pipelines. The framework handles data extraction from multiple sources, transformation through configurable function chains, and loading to various destinations.

Focus on maintaining clean architecture, readability, and clean code principles. Always adhere to design patterns and principles like:
- Single Responsibility Principle (SRP)
- Open/Closed Principle (OCP)
- Dependency Inversion
- Don't Repeat Yourself (DRY)
- SOLID principles
- Composition over Inheritance
- Separation of concerns

Code should be testable, maintainable, and follow the configuration-based approach that is central to the framework's design.

Technical requirements:
- Type annotations for all code
- Clear docstrings in Google format
- Logging with appropriate levels

Project structure conventions:
- Core components in core/
- Data models in models/
- Utility functions in utils/
