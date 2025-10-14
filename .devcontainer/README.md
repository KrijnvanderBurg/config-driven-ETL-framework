# .devcontainer
This repository contains several specialized DevContainers for different development environments. It's designed to be included as a Git submodule in your projects.

> **Note**: The `.devcontainer` folder might be invisible in some file explorers due to the leading dot (`.`) in its name. Ensure your file explorer is configured to show hidden files and folders.

## Available Containers
### [Python -Code Quality- DevContainer](./python-spark)
A complete Python development environment with:

- Python 3.11 runtime and Poetry package manager
- Comprehensive linting and static analysis tools (Ruff, PyLint, Flake8, MyPy)
- Code quality tools (Bandit, DevSkim, Semgrep)
- Testing configuration with pytest and coverage reporting
- Pre-configured VS Code tasks for all development workflows

### [Spark Cluster DevContainer](./spark-cluster/)
A complete Apache Spark development environment:

- Fully configured Spark cluster (master, workers, history server)
- PySpark integration with Python
- Jupyter notebook support with pre-configured Spark session
- One-click Spark job submission via VS Code tasks
- Web UIs for monitoring Spark clusters and jobs

### [OpenTofu DevContainer (in development)](./opentofu)
An environment for infrastructure as code development:

- OpenTofu (open-source Terraform alternative) pre-installed
- Azure CLI integration
- Security scanning tools
- VS Code tasks for common OpenTofu operations (init, plan, validate)

> **🚀 Ready to deploy with confidence?** These DevContainers use identical configurations as the [Azure DevOps CI/CD templates](https://github.com/KrijnvanderBurg/.azuredevops). Test your code quality, security scans, and deployments locally — **guaranteed consistency** means your pipeline will pass when local checks do.



## 🏁 Getting Started

### Prerequisites
- [Docker Desktop](https://www.docker.com/products/docker-desktop/) installed and running
- [VS Code](https://code.visualstudio.com/) with the [Dev Containers extension](https://marketplace.visualstudio.com/items?itemName=ms-vscode-remote.remote-containers) installed

### Quick Installation

1. **Add this repository as a Git submodule to your project:**
```bash
git submodule add https://github.com/KrijnvanderBurg/.devcontainer.git .devcontainer
git submodule update --init --recursive
```

> **Important**: The `--recursive` flag is **required** because this DevContainer uses a nested submodule. The [`.dotfiles`](https://github.com/KrijnvanderBurg/.dotfiles) directory within this repository is itself a submodule containing shared configuration files and scripts that ensure consistency between local development and CI/CD environments. Details in chapter [Architecture](#%EF%B8%8F-architecture-shared-configuration-submodule-dotfiles).

2. **Build and start the DevContainer**:
   - Press `F1` to open the command palette
   - Type and select `Dev Containers: Rebuild and Reopen in Container`
   - VS Code will build the Docker images and start the containers defined in `docker-compose.yml`
   - This process may take several minutes the first time



## 🏗️ Architecture: Shared Configuration Submodule `.dotfiles`

This DevContainer repository uses a **nested [`.dotfiles`](https://github.com/KrijnvanderBurg/.dotfiles) submodule architecture** to ensure **100% consistency** between local development environments and CI/CD pipelines.

### Why Nested Submodules?

The `.dotfiles` directory is itself a Git submodule that contains:
- **Configuration files** for all development tools (linters, formatters, type checkers, security scanners)
- **Shell scripts** that execute each tool with identical parameters and settings
- **Shared standards** across multiple environments

### Consistency Across Environments

This same `.dotfiles` submodule is used by **other DevOps components**:

- **`.devcontainer`** (this repository) - Local development environments
- **[`.azuredevops`](https://github.com/krijnvanderburg/.azuredevops)** - CI/CD pipeline templates and automation
- `...` other future submodules

### Benefits of This Architecture

🎯 **Identical Tool Execution**: The exact same shell script runs both locally in DevContainers and in CI/CD pipelines  
🔒 **Configuration Consistency**: All environments use identical configuration files for every tool  
🚀 **Quality Gate Confidence**: If a tool passes locally, it will pass in CI/CD (and vice versa)  
⚡ **Single Source of Truth**: Update tool configurations once, apply everywhere  
🛠️ **Easier Maintenance**: Modify scripts and configs in one place, benefit all environments  

### How It Works: Pylint example
When you run a tool locally in VS Code it uses a script and config file from `.dotfiles`.
```jsonc
// Example VSCode Task
{
   "label": "pylint",
   //              The Nested Submodule .dotfiles  ⤵
   "command": "${workspaceFolder}/scripts/pylint.sh", // ← Same script
   "args": [
      "${workspaceFolder}/src",                                                 // ← Same target dirpath
      "--config ${workspaceFolder}/.github/config/.pylintrc"    // ← Same config file
   ],
   ...
}
```

When the CI/CD pipeline (Azure Devops template) runs the same tool it uses the same script and config file from `.dotfiles`:
```js
// Example Azure Devops pipeline step
steps:
- script: |
    //               The Nested Submodule .dotfiles  ⤵
    sh $(Build.Repository.LocalPath)/.azuredevops/.dotfiles/python/scripts/pylint.sh \ // ← Same script
      $(Build.Repository.LocalPath)/src \                                            // ← Same dirpath
      --config $(Build.Repository.LocalPath)/.azuredevops/.dotfiles/python/.pylintrc // ← Same config
  displayName: Pylint (linter)
  ...
```

This ensures that **quality gates in CI/CD match your local experience exactly**; eliminating waiting for CICD runs to see if it conforms to set quality gates.

---

## 🚀 Next Steps: Complete DevOps Integration

**See the Big Picture:** Explore the [complete DevOps Toolkit](https://github.com/KrijnvanderBurg/DevOps-Toolkit) to understand how local development, CI/CD, and shared configurations create a **unified development experience**.

**Deploy with Confidence:** Use the [Azure DevOps CI/CD templates](https://github.com/KrijnvanderBurg/.azuredevops) that mirror these DevContainer environments. **25+ pipeline templates** with the same tools and configurations for seamless local-remote workflows.

**Master the Architecture:** Deep dive into the [shared configuration strategy](https://github.com/KrijnvanderBurg/.dotfiles) that powers this consistency. See how **one script, one config** eliminates environment drift forever.
