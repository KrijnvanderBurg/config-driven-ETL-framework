# Python DevContainer - Complete Code Quality Suite

A development environment with 20+ integrated code quality tools, automated CI/CD validation, and zero-configuration setup. This DevContainer provides everything you need for professional Python development: formatters, linters, type checkers, security scanners, dependency analyzers, test runners, and documentation generators.

## 🚀 Why Use This DevContainer?

### Zero-Configuration Professional Development Environment
DevContainers provide a fully configured and isolated development environment inside a container, ensuring consistent, reproducible, and platform-independent setup across your entire team. No more "works on my machine" issues or lengthy environment setup procedures.

### Comprehensive Tool Integration
This DevContainer includes 20+ carefully curated industry-standard Python development tools:

- **🎨 Code Formatters**: Ruff, Black, isort - Ensure consistent code style across your team
- **🔍 Linters & Analyzers**: Pylint, Flake8, Ruff Linter - Catch bugs and enforce best practices
- **🔒 Type Checkers**: Mypy, Pyright, Pyre, Pylance - Prevent type-related errors before runtime
- **🛡️ Security Scanners**: Bandit, Semgrep, DevSkim - Identify vulnerabilities and security issues
- **🔐 Secret Detection**: Gitleaks, TruffleHog - Prevent credential leaks in your repository
- **📦 Dependency Analysis**: OWASP Dependency Check, OSSAudit - Monitor third-party package vulnerabilities
- **🧪 Testing & Coverage**: Pytest with Coverage.py - Ensure code reliability and test completeness
- **📚 Documentation**: Sphinx - Generate professional documentation automatically

### Two Modes of Operation

#### Primary: Real-time Code Analysis (Opened Files)
Extensions provide instant feedback as you code, showing issues directly in the editor and Problems tab for the currently open file.

!["Preview of live problems tab showing issues"](./docs/preview_live_problems_tab.gif)

Code Coverage can be seen directly in the editor after running tests. The `if` line shows orange for partially covered by unit tests and the `print` line shows red for not covered.

![Preview Code Coverage in Editor](./docs/preview_coverage_in_editor.gif)


#### Secondary: Complete Repository Validation (All Files)
VS Code tasks run comprehensive analysis across your entire repository, simulating CI/CD pipeline checks locally.

!["Run all Code Quality tools as VSCode Tasks"](./docs/run_all_vscode_tasks.gif)

### Quick Setup

**Launch**: Open project in VS Code → `F1` → "Dev Containers: Reopen in Container"

### Architecture Benefits

This implementation uses Docker Compose for maximum flexibility:
- **Main Container**: Development environment with all tools pre-installed
- **Extensible**: Easy to add supporting services (databases, message queues, etc.)
- **Volume Mounts**: Persistent storage for workspace and configuration
- **Platform Independent**: Consistent experience across Windows, macOS, and Linux

## 🏁 Getting Started

### Prerequisites
- [Docker Desktop](https://www.docker.com/products/docker-desktop) installed and running
- [Dev Containers extension](https://marketplace.visualstudio.com/items?itemName=ms-vscode-remote.remote-containers) for VS Code

### Quick Installation

1. **📥 Clone This Example Repository**: 
   ```bash
   git clone https://github.com/KrijnvanderBurg/DevOps-Toolkit
   cd DevOps-Toolkit
   ```

2. **📦 Initialize Submodules** (Required):
   ```bash
   git submodule update --init --recursive
   ```
   
   **Why `--recursive`?** The `.devcontainer/.dotfiles` directory is a git submodule from another repository. This ensures all configuration files and scripts are properly downloaded. If you create a submodule of this devcontainer then its a nested submodule. 

3. **🐳 Launch Container**: Open project in VS Code, press `F1` → "Dev Containers: Reopen in Container"

4. **⚡ Verify Setup**: `Ctrl+Shift+P` → "Tasks: Run Task" → "test" (all tools should pass on a fresh project)

### Architecture Notes

- **Nested Repository Structure**: The `.devcontainer/.dotfiles` is a git submodule containing all tool configurations and scripts as a self-contained unit
- **Why Submodule**: This approach keeps the DevContainer configuration portable and reusable across multiple projects while maintaining version control over the development environment setup

> **🚀 Ready for production deployment?** This DevContainer uses identical configurations as the [Azure DevOps CI/CD templates](https://github.com/KrijnvanderBurg/.azuredevops). Your local quality checks **mirror your pipeline exactly** — when tests pass locally, they're guaranteed to pass in production.

**Pro Tips:**
- Run `test` task before committing to catch issues early
- Use individual tool tasks for focused debugging  
- Check the Problems panel for real-time feedback on open files
- Generated reports (`coverage.xml`, `JUNIT-TEST.xml`) provide detailed analysis

## 🛠️ List of Code Quality Tools

### Formatters

- **Ruff Formatter** <sup>[Docs](https://docs.astral.sh/ruff/) | [Github](https://github.com/astral-sh/ruff) | [VS Code Marketplace](https://marketplace.visualstudio.com/items?itemName=charliermarsh.ruff)</sup> - Lightning-fast Python formatter written in Rust, combining the functionality of Black, isort, and more. **Primary recommendation** for modern Python development.

- **Black** <sup>[Docs](https://black.readthedocs.io/en/stable/) | [Github](https://github.com/psf/black) | [VS Code Marketplace](https://marketplace.visualstudio.com/items?itemName=ms-python.black-formatter)</sup> - The uncompromising Python code formatter that eliminates debates about code style.

- **isort** <sup>[Docs](https://pycqa.github.io/isort/) | [Github](https://github.com/PyCQA/isort) | [VS Code Marketplace](https://marketplace.visualstudio.com/items?itemName=ms-python.isort)</sup> - Sorts Python imports alphabetically and automatically separates them into sections.

### Linters & Code Quality

- **Ruff Linter** <sup>[Docs](https://docs.astral.sh/ruff/) | [Github](https://github.com/astral-sh/ruff) | [VS Code Marketplace](https://marketplace.visualstudio.com/items?itemName=charliermarsh.ruff)</sup> - Extremely fast linter with 800+ rules, replacing Flake8, pylint, and more. **Primary recommendation** for comprehensive code quality checks.

- **Pylint** <sup>[Docs](https://pylint.org/) | [Github](https://github.com/pylint-dev/pylint) | [VS Code Marketplace](https://marketplace.visualstudio.com/items?itemName=ms-python.pylint)</sup> - Comprehensive static code analyzer that checks for errors, enforces coding standards, and suggests refactorings.

- **Flake8** <sup>[Docs](https://flake8.pycqa.org/) | [Github](https://github.com/PyCQA/flake8) | [VS Code Marketplace](https://marketplace.visualstudio.com/items?itemName=ms-python.flake8)</sup> - Combines pycodestyle, pyflakes, and mccabe to check code style and quality.


### Type Checkers

- **Pylance** <sup>[Github](https://github.com/microsoft/pylance-release) | [VS Code Marketplace](https://marketplace.visualstudio.com/items?itemName=ms-python.vscode-pylance)</sup> - Microsoft's fast, feature-rich Python language server powered by Pyright. Provides IntelliSense, type checking, and code navigation.

- **Mypy** <sup>[Docs](https://mypy-lang.org/) | [Github](https://github.com/python/mypy) | [VS Code Marketplace](https://marketplace.visualstudio.com/items?itemName=ms-python.mypy-type-checker)</sup> - Optional static type checker that combines dynamic and static typing benefits.

- **Pyright** <sup>[Docs](https://microsoft.github.io/pyright) | [Github](https://github.com/microsoft/pyright) | [VS Code Marketplace](https://marketplace.visualstudio.com/items?itemName=ms-pyright.pyright)</sup> - Fast, standards-based static type checker designed for large Python codebases.

- **Pyre** <sup>[Docs](https://pyre-check.org/) | [Github](https://github.com/facebook/pyre-check)</sup> - Facebook's performant type checker that provides incremental analysis for large codebases.

### Security & Vulnerability Scanners

- **Bandit** <sup>[Docs](https://bandit.readthedocs.io) | [Github](https://github.com/PyCQA/bandit) | [VS Code Marketplace](https://marketplace.visualstudio.com/items?itemName=nwgh.bandit)</sup> - Scans Python code for common security issues by analyzing AST patterns.

- **Semgrep** <sup>[Docs](https://semgrep.dev/p/python) | [Github](https://github.com/semgrep/semgrep)</sup> - Fast static analysis tool for finding bugs, security vulnerabilities, and enforcing code standards.

- **DevSkim** <sup>[Docs](https://github.com/microsoft/DevSkim/wiki) | [Github](https://github.com/microsoft/DevSkim) | [VS Code Marketplace](https://marketplace.visualstudio.com/items?itemName=MS-CST-E.vscode-devskim)</sup> - Microsoft's security analysis framework for detecting vulnerabilities as you code.

### Secrets & Credentials Detection

- **TruffleHog** <sup>[Github](https://github.com/trufflesecurity/trufflehog)</sup> - Searches through git repositories for high-entropy strings and secrets, detecting passwords, API keys, and tokens.

- **Gitleaks** <sup>[Github](https://github.com/gitleaks/gitleaks)</sup> - SAST tool for detecting and preventing hardcoded secrets in git repositories.

### Dependency Analysis

- **Ossaudit** <sup>[Github](https://github.com/illikainen/ossaudit) | [Pypi](https://pypi.org/project/ossaudit/)</sup> - Ossaudit uses Sonatype OSS Index to audit Python packages for known vulnerabilities. [Config file](../.dotfiles/python/ossaudit.yaml) | [AzDO pipeline](https://github.com/KrijnvanderBurg/.azuredevops/tree/main/v1/templates/atomic/python/ossaudit.yaml)

- **OWASP dependency-check** <sup>[Docs](https://owasp.org/www-project-dependency-check/) | [Github](https://github.com/jeremylong/DependencyCheck) | [Pypi](https://pypi.org/project/dependency-check/)</sup> - Dependency-Check is a Software Composition Analysis (SCA) tool that attempts to detect publicly disclosed vulnerabilities contained within a project’s dependencies. It does this by determining if there is a Common Platform Enumeration (CPE) identifier for a given dependency. If found, it will generate a report linking to the associated CVE entries. [Config file](../.dotfiles/python/cve_suppressions.xml) | [AzDO pipeline](https://github.com/KrijnvanderBurg/.azuredevops/tree/main/v1/templates/atomic/python/owasp.yaml)

- **Safety** (Paid tool, not implemented) <sup>[Docs](https://docs.safetycli.com/) | [Github](https://github.com/pyupio/safety) | [Pypi](https://pypi.org/project/safety/)</sup> - Safety CLI is a Python dependency vulnerability scanner designed to enhance software supply chain security by detecting packages with known vulnerabilities and malicious packages in local development environments, CI/CD, and production systems. Safety CLI can be deployed in minutes and provides clear, actionable recommendations for remediation of detected vulnerabilities.

### Testing

- **Pytest** <sup>[Docs](https://docs.pytest.org) | [Github](https://github.com/pytest-dev/pytest) | [Pypi](https://pypi.org/project/pytest/)</sup> - The pytest framework makes it easy to write small tests, yet scales to support complex functional testing for applications and libraries. [Config file](../.dotfiles/python/pytest.ini) | [AzDO pipeline](https://github.com/KrijnvanderBurg/.azuredevops/tree/main/v1/templates/atomic/python/test_pytest.yaml) | [DevContainer config](#pytest)

- **Code Coverage** <sup>[coverage.py](https://coverage.readthedocs.io/)</sup>: Checks code coverage of Python tests with coverage.py. [Config file](../.dotfiles/python/.coveragerc) | [AzDO pipeline](https://github.com/KrijnvanderBurg/.azuredevops/tree/main/v1/templates/atomic/python/pytest.yaml) | [DevContainer config](#pytest)


### Dependency management

- **Poetry** <sup>[Docs](https://python-poetry.org/) | [Github](https://github.com/python-poetry/poetry) | [Pypi]()</sup> - Poetry helps you declare, manage and install dependencies of Python projects, ensuring you have the right stack everywhere. [AzDO pipeline](https://github.com/KrijnvanderBurg/.azuredevops/tree/main/v1/templates/atomic/python/poetry-install-pyproject.yaml)

- **Pip** <sup>[Docs](https://pip.pypa.io) | [Github](https://github.com/pypa/pip) | [Pypi](https://pypi.org/project/pip/)</sup> - pip is the package installer for Python. You can use pip to install packages from the Python Package Index and other indexes. [AzDO pipeline](https://github.com/KrijnvanderBurg/.azuredevops/tree/main/v1/templates/atomic/python/pip-install-requirements.yaml)

### Packaging

- **Python Build** <sup>[Website](https://packaging.python.org) | [Docs](https://build.pypa.io) | [Pypi](https://pypi.org/project/build/)</sup> - A simple, correct Python packaging build frontend. [AzDO pipeline](https://github.com/KrijnvanderBurg/.azuredevops/tree/main/v1/templates/atomic/python/build_wheel_bdist.yaml)

- **Twine** <sup>[Docs](https://twine.readthedocs.io) | [Github](https://github.com/pypa/twine) | [Pypi](https://pypi.org/project/twine/)</sup> - Twine is a utility for publishing Python packages on PyPI. It provides build system independent uploads of source and binary distribution artifacts for both new and existing projects. [AzDO pipeline](https://github.com/KrijnvanderBurg/.azuredevops/tree/main/v1/templates/atomic/python/twine-upload.yaml)

### Profilers (TODO)
- **cProfile**: Profiles Python code with cProfile. [cProfile](https://docs.python.org/3/library/profile.html)
- **py_spy**: Profiles Python code with py-spy. [py-spy](https://github.com/benfred/py-spy)
- **line_profiler**: Profiles Python code with line_profiler. [line_profiler](https://pypi.org/project/)
- **memory_profiler**: Profiles memory usage of Python code with memory-profiler. [memory-profiler](https://pypi.org/project/memory-profiler/)
- **plop**: Profiles Python code with Plop. [Plop](https://pypi.org/project/plop/)
- **vprof**: Profiles Python code with vprof. [vprof](https://github.com/nvdv/vprof)
- **yappi**: Profiles Python code with Yappi. [Yappi](https://pypi.org/project/yappi/)

### Documentation

- **Sphinx** <sup>[Docs](https://www.sphinx-doc.org/) | [Github](https://github.com/sphinx-doc/sphinx) | [Pypi](https://pypi.org/project/Sphinx/)</sup> - Sphinx makes it easy to create intelligent and beautiful documentation. [Config file](hello_world/docs/) | [AzDO pipeline](https://github.com/KrijnvanderBurg/.azuredevops/tree/main/v1/templates/atomic/python/sphinx.yaml)

## ⚙️ VS Code Extensions Configuration

This DevContainer automatically installs and configures all necessary VS Code extensions. This section provides technical details about extension settings for developers who want to understand or customize the configuration.

### 🧩 Why Use VS Code Extensions?
Extensions improve your development workflow by providing real-time feedback, automated formatting, security scanning, and advanced code navigation—all directly in your editor. Properly configuring these extensions ensures that every team member benefits from consistent tooling, immediate issue detection, and seamless integration with your DevContainer setup.

!["Preview of live problems tab showing issues"](./docs/preview_live_problems_tab.gif)

#### 🎨 Formatting Extensions

##### Ruff (Primary Formatter)
Modern, fast formatter combining Black, isort, and more. Automatically runs on file save.

```json
"extensions": [
    "charliermarsh.ruff"
],
"settings": {
    "[python]": {
        "editor.formatOnSave": true,
        "editor.codeActionsOnSave": {
            "source.fixAll": "explicit",
            "source.organizeImports": "explicit"
        },
        "editor.defaultFormatter": "charliermarsh.ruff"
    },
    "ruff.lint.args": [
        "--config=${workspaceFolder}/path/to/.ruff.toml"
    ],
    "ruff.format.args": [
        "--config=${workspaceFolder}/path/to/.ruff.toml"
    ],
},
```

##### Black (Alternative Formatter)
Uncompromising Python code formatter. Runs on file save.

```json
"extensions": [
    "ms-python.black-formatter"
],
"settings": {
    "[python]": {
        "editor.defaultFormatter": "ms-python.black-formatter",
        "editor.formatOnSave": true,
    },
    "black-formatter.args": [
        "--config",
        "${workspaceFolder}/path/to/.black"
    ],
    "black-formatter.cwd": "${workspaceFolder}",
    "black-formatter.enabled": true,
    "black-formatter.showNotification": "onError"
},
```

##### Isort (Import Organizer)
Sorts and organizes Python imports. Runs on file save.

```json
"extensions": [
    "ms-python.isort"
],
"settings": {
    "[python]": {
        "editor.formatOnSave": true,
        "editor.codeActionsOnSave": {
            "source.organizeImports": "explicit"
        }
    },
    "isort.args": [
        "--settings-path",
        "${workspaceFolder}/path/to/.isort.cfg"
    ],
    "isort.serverEnabled": false,
    "isort.check": false,
    "isort.showNotification": "onError"
},
```

#### 🔍 Linting Extensions

##### Pylint
Comprehensive code analyzer. Shows issues in editor and Problems tab.

```json
"extensions": [
    "ms-python.pylint"
],
"settings": {

    "pylint.args": [
        "--rcfile",
        "${workspaceFolder}/path/to/.pylintrc"
    ],
    "pylint.cwd": "${workspaceFolder}",
    "pylint.enabled": true,
    "pylint.showNotification": "onError",
    "pylint.lintOnChange": true,
},
```

##### Flake8  
Style and quality checker. Shows issues in editor and Problems tab.

```json
"extensions": [
    "ms-python.flake8"
],
"settings": {
    "flake8.args": [
        "--config",
        "${workspaceFolder}/path/to/.flake8" 
    ],
    "flake8.cwd": "${workspaceFolder}",
    "flake8.showNotification": "onError",
    "flake8.enabled": true,
},
```

#### 🛡️ Security Extensions

##### Bandit
Security vulnerability scanner. Shows issues in editor and Problems tab.

```json
"extensions": [
    "nwghbandit.ini"
],
"settings": {
    "bandit.args": [
        "-c",
        "${workspaceFolder}/path/to/bandit.ini"
    ],
    "bandit.cwd": "${workspaceFolder}",
    "bandit.logLevel": "warning",
    "bandit.enabled": true,
    "bandit.showNotification": "onError"
},
```

##### DevSkim
Microsoft's security analysis framework. Shows security issues in editor.

*Note: Limited configuration options available.*

```json
"extensions": [
    "ms-cst-e.vscode-devskim"
],
"settings": {},
```

#### 🔧 Type Checking Extensions

##### Mypy
Static type checker. Shows type issues in editor and Problems tab.

```json
"extensions": [
    "ms-python.flake8"
],
"settings": {
    "flake8.args": [
        "--config",
        "${workspaceFolder}/path/to/.flake8"
    ],
    "flake8.cwd": "${workspaceFolder}",
    "flake8.showNotification": "onError",
    "flake8.enabled": true,
    // ...
},
```

#### 🧪 Testing Extensions

##### Pytest
Test discovery and execution. Access via Testing panel in VS Code sidebar.

```json
"extensions": [],
"settings": {
    "python.testing.unittestEnabled": false,
    "python.testing.pytestEnabled": true,
    "python.testing.pytestArgs": [
        "-c",
        "${workspaceFolder}/path/to/pytest.ini"
    ],
    "python.languageServer": "Default",
```

##### Pylance (IntelliSense)
Microsoft's Python language server. Provides IntelliSense, type checking, and code navigation.

```json
"extensions": [
    "ms-python.vscode-pylance"
],
"settings": {
    "python.analysis.typeCheckingMode": "strict",
    "python.analysis.diagnosticMode": "workspace",
    "python.analysis.diagnosticSeverityOverrides": {
        "reportUnknownParameterType": false,
        "reportUnknownArgumentType": false,
        "reportUnknownVariableType": false,
        "reportUnknownMemberType": false,
        "reportMissingParameterType": false,
        "reportMissingTypeArgument": false,
        "reportGeneralTypeIssues": false
    },
    "python.analysis.completeFunctionParens": true,
    "python.analysis.inlayHints.variableType": true,
    "python.analysis.inlayHints.functionReturnTypes": true,
    "python.analysis.inlayHints.callArgumentName": true,
    "python.analysis.inlayHints.pytestParameters": true,
},
```

## 📊 Code Coverage Visualization

The DevContainer includes visual code coverage display that works with both VS Code's built-in test runner and task-based testing. Below shows an if statement being only partially covered by tests. The `if` line shows orange for partially covered by unit tests and the `print` line shows red for not covered.

![Preview Code Coverage in Editor](./docs/preview_coverage_in_editor.gif)

### How Coverage Works

Both testing methods generate a `coverage.xml` file in the workspace root:

- **VS Code UI Tests**: Run tests via the Testing panel (sidebar) → automatically generates `coverage.xml`
- **VS Code Tasks**: Run `pytest and coverage` task → generates `coverage.xml` and `JUNIT-TEST.xml`

### Coverage Gutter Extension

Visual code coverage display shows covered/uncovered lines directly in editor gutters.

**Setup:**
1. Run tests with coverage using either method above
2. Open Command Palette: `Ctrl+Shift+P`
3. Run: "Coverage Gutter: Display Coverage"
4. **Enable Watch Mode**: "Coverage Gutter: Watch" for automatic updates when `coverage.xml` changes

**Benefits:**
- **Real-time Feedback**: See coverage immediately after running tests
- **Visual Indicators**: Green (covered), red (uncovered), yellow (partial) line highlighting
- **Automatic Updates**: Watch mode refreshes coverage when you re-run tests

**Troubleshooting:**
- Ensure `coverage.xml` is generated in workspace root
- Check `.coveragerc` configuration if coverage isn't working
- Verify pytest is configured with coverage in `pytest.ini`

## 🔧 VS Code Tasks - Complete CI/CD Pipeline Integration

This DevContainer includes a comprehensive task automation system that mirrors your CI/CD pipeline, allowing you to validate your entire codebase locally before pushing to remote repositories.

### Why Use Tasks?

**Repository-Wide Analysis**: Unlike live editor feedback (limited to open files), tasks scan your entire codebase for comprehensive validation.  
**Pre-Push Confidence**: Catch issues locally using the same tools and configurations as your production CI/CD pipeline.  
**Parallel Execution**: Independent tasks can run simultaneously for faster feedback.

!["Run all Code Quality tools as VSCode Tasks"](./docs/run_all_vscode_tasks.gif)

### How to Run Tasks

**Command Palette**: `Ctrl+Shift+P` → `"Tasks: Run Task"` → Select task  
**Terminal Menu**: `Terminal` → `Run Task...`

### Available Tasks

#### 🏗️ Build Tasks
- **poetry lock**: Lock dependencies
- **poetry install**: Install dependencies  
- **build package**: Create Python wheel
- **install package**: Install the built package

#### ✅ Test Tasks  
- **sphinx build docs**: Generate documentation
- **ruff formatter**: Format code with Ruff
- **black**: Format code with Black
- **isort**: Sort imports
- **ruff linter**: Lint with Ruff
- **pylint**: Lint with Pylint
- **flake8**: Lint with Flake8
- **mypy**: Type check with MyPy
- **pyright**: Type check with Pyright
- **pyre**: Type check with Pyre
- **bandit**: Security scan with Bandit
- **semgrep**: Security scan with Semgrep
- **trufflehog**: Secret detection
- **pytest and coverage**: Run tests with coverage

### Results & Output
- **Terminal Panels**: Each task runs in a dedicated terminal
- **Error Indicator**: Red text with ❌ for failures
- **Generated Reports**: `coverage.xml`, `JUNIT-TEST.xml` in workspace root

### Technical Implementation

All tasks execute shell scripts from `.devcontainer/.dotfiles/python/scripts/` using centralized configuration files. This ensures consistency between local development and CI/CD environments.

### Configuration File Paths

All tools reference centralized configuration files:
```
.devcontainer/.dotfiles/python/
└── scripts/              # Execution scripts
    ├── ruff_formatter.sh
    ├── pylint.sh
    └── ...
├── ruff.toml            # Ruff configuration
├── mypy.ini             # MyPy type checker settings
├── pytest.ini           # Pytest configuration
├── ...
```

### Adding New Tasks

To add a new tool:

1. **Add configuration**: Create tool config file in `.dotfiles/`
2. **Create script**: Add execution script to `.dotfiles/.../scripts/` directory
3. **Define task**: Add task definition to `devcontainer.json`
4. **Update dependencies**: Add to appropriate task chain

### Task Debugging

For troubleshooting task execution:
- Check script permissions (`chmod +x`)
- Verify file paths and variable substitution
- Test scripts manually in terminal
- Review task output in dedicated terminal panels

This comprehensive task system provides a robust foundation for maintaining code quality and ensuring CI/CD pipeline compatibility.

## ⚙️ Platform Configuration

#### Git Configuration (Recommended)
Add the following lines to `.gitattributes` file in project root. This prevents git from thinking ALL files changed due to different line endings when opening devcontainer from Windows machines:
```bash
* text=auto
*.sh text eol=lf
*.conf text eol=lf
```

#### WSL2 Resource Limits (Optional)
Dev containers use WSL2 on Windows, which can consume significant resources. You can define limits in `.wslconfig` and place it at `C:\Users\<username>\.wslconfig`:

```ini
[wsl2]
# Limits VM memory to use no more than 6 GB
memory=6GB 
# Sets the VM to use four virtual processors
processors=4
# Sets amount of swap storage space to 8GB, default is 25% of available RAM
swap=8GB
# Enable page reporting so WSL releases memory back to Windows when free
pageReporting=true
```

---

## 🚀 Take Your Development Further

**Scale to Production:** Deploy with the [Azure DevOps CI/CD templates](https://github.com/KrijnvanderBurg/.azuredevops) that use identical configurations. **25+ pipeline templates** ensure seamless local-to-production workflows with zero surprises.

**Understand the Magic:** Explore the [shared configuration architecture](https://github.com/KrijnvanderBurg/.dotfiles) that makes local-remote consistency possible. See how **one script, one config** eliminates environment drift.

**Complete Integration:** Check out the [full DevOps Toolkit](https://github.com/KrijnvanderBurg/DevOps-Toolkit) for the complete picture of how DevContainers, CI/CD, and shared configs create a **unified development experience**.
