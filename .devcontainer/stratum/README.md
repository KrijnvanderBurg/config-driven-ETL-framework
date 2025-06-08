# Instructions DevContainer
1. <kbd>ctrl</kbd> + <kbd>shift</kbd> + <kbd>P</kbd>: `Dev Containers: Rebuild Container`
2. Open VScode folder in Dev Container via either:
    - <kbd>ctrl</kbd> + <kbd>shift</kbd> + <kbd>P</kbd> : Dev Containers: Open Container
    - Click on `remote window` button, light blue in bottom-left corner of VScode.

# WSL config
Dev containers use WSL2. This can use a lot of resources, limits can be defined in `.wslconfig` and placed in `C:\Users\<username>\.wslconfig`. See `./.devcontainer/.wslconfig` for example config.

# Included tools

## Coverage Gutter
1. Generate a code coverage using:
    ```python
    pytest ./ -s --cache-clear -c ./.devcontainer/pytest.ini --cov ./ --cov-report xml:./coverage.xml --cov-config ./.devcontainer/.coveragerc
    ```
    (requires `pytest`, `pytest-cov`)
    
2. <kbd>ctrl</kbd> + <kbd>shift</kbd> + <kbd>P</kbd>: Coverage Gutter: Display Coverage. Or toggle `Coverage Gutter: Watch`, this will watch the coverage.xml for changes. 

    Though this might give error pop-ups as it might try to read while coverage.xml is still being generated.

If no coverage.xml is generated then its likely an issue with `.coveragerc`.

### Generate coverage on file save (not working)
Using these settings for Python/pytest extension, a coverage.xml file is generated on each <kbd>file save</kbd> action. However, the result coverage.xml shown by `Coverage Gutters` is entirely incorrect. The `coverage.xml` is not generated properly on <kbd>file save</kbd> action as the actual code coverage is incorrect.

settings in `devcontainer.json`
```json
"extensions": [
    "ryanluker.vscode-coverage-gutters",
    // ...
],
"settings": {
    //
    // Python/Pytest - https://code.visualstudio.com/docs/python/settings-reference
    //
    "python.testing.unittestEnabled": false,
    "python.testing.pytestEnabled": true,
    "python.testing.pytestArgs": [
        "-c",
        "${workspaceFolder}/.devcontainer/pytest.ini",
        "--cov",
        "${workspaceFolder}",
        "--cov-report",
        "xml:${workspaceFolder}/coverage.xml",
        "--cov-config",
        "${workspaceFolder}/.devcontainer/.coveragerc"
    ],
```

## Pylance
Code validator and type checker, runs automatically and shows its result in the `problems` section of VScode integrated terminal.

settings in `devcontainer.json`
```json
"extensions": [
    "ms-python.vscode-pylance", // https://marketplace.visualstudio.com/items?itemName=ms-python.vscode-pylance
    // ...
],
"settings": {
    //
    // Pylance - https://marketplace.visualstudio.com/items?itemName=ms-python.vscode-pylance
    //
    "python.analysis.typeCheckingMode": "strict", // 'off' (default), 'basic', 'strict'
    "python.analysis.diagnosticMode": "workspace", // 'openFilesOnly' (default), 'workspace'
    "python.analysis.diagnosticSeverityOverrides": {
        // https://github.com/microsoft/pyright/blob/main/docs/configuration.md#type-check-diagnostics-settings
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

## Pylance
Code validator and type checker. Runs automatically for all files. Shows its result in the `problems` section of VScode integrated terminal.

settings in `devcontainer.json`
```json
"extensions": [
    "ms-python.vscode-pylance", // https://marketplace.visualstudio.com/items?itemName=ms-python.vscode-pylance
    // ...
],
"settings": {
    //
    // Pylance - https://marketplace.visualstudio.com/items?itemName=ms-python.vscode-pylance
    //
    "python.analysis.typeCheckingMode": "strict", // 'off' (default), 'basic', 'strict'
    "python.analysis.diagnosticMode": "workspace", // 'openFilesOnly' (default), 'workspace'
    "python.analysis.diagnosticSeverityOverrides": {
        // https://github.com/microsoft/pyright/blob/main/docs/configuration.md#type-check-diagnostics-settings
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
    // ...
},
```

## Black
Code formatter of pep8. Automatically runs on <kbd>file save</kbd> action for opened file.

settings in `devcontainer.json`
```json
"extensions": [
    "ms-python.black-formatter", // https://marketplace.visualstudio.com/items?itemName=ms-python.black-formatter
    // ...
],
"settings": {
    "[python]": {
        "editor.defaultFormatter": "ms-python.black-formatter",
        "editor.formatOnSave": true,
    },
    //
    // Black - https://marketplace.visualstudio.com/items?itemName=ms-python.black-formatter
    //
    "black-formatter.args": [
        "--config",
        "${workspaceFolder}/.devcontainer/.black"
    ],
    "black-formatter.cwd": "${workspaceFolder}",
    "black-formatter.enabled": true,
    "black-formatter.showNotification": "onError", // 'off' (default), 'onWarning', 'onError', 'always'
    // ...
},
```

## Pylint
Code formatter of Pep8 and more. Automatically runs on <kbd>file save</kbd> action for opened file. Shows its result in code UI and in the `problems` section of VScode integrated terminal.

settings in `devcontainer.json`
```json
"extensions": [
    "ms-python.pylint", // https://marketplace.visualstudio.com/items?itemName=ms-python.pylint
    // ...
],
"settings": {
    //
    // Pylint - https://marketplace.visualstudio.com/items?itemName=ms-python.pylint
    //
    "pylint.args": [
        "--rcfile",
        "${workspaceFolder}/.devcontainer/.pylintrc"
    ],
    "pylint.cwd": "${workspaceFolder}",
    "pylint.enabled": true,
    "pylint.showNotification": "onError", // 'off' (default), 'onWarning', 'onError', 'always'
    "pylint.lintOnChange": true,
    // ...
},
```

## Isort
Import organizer of Pep8. Automatically runs on <kbd>file save</kbd> action for opened file. Press <kbd>Alt</kbd>+<kbd>shift</kbd>+<kbd>O</kbd> to manually run for currently open file. 

settings in `devcontainer.json`
```json
"extensions": [
    "ms-python.isort", // https://marketplace.visualstudio.com/items?itemName=ms-python.isort
    // ...
],
"settings": {
    // Python
    "[python]": {
        "editor.formatOnSave": true,
        "editor.codeActionsOnSave": {
            "source.organizeImports": "explicit"
        }
    },
    //
    // Isort - https://marketplace.visualstudio.com/items?itemName=ms-python.isort
    //
    "isort.args": [
        "--settings-path",
        "${workspaceFolder}/.devcontainer/.isort.cfg"
    ],
    "isort.serverEnabled": false,
    "isort.check": false,
    "isort.showNotification": "onError", // 'off' (default), 'onWarning', 'onError', 'always'
    // ...
},
```
				
## Flake8
Linter of Pep8 and more. Automatically runs on <kbd>file save</kbd> action for opened file. Shows its result in code UI and in the `problems` section of VScode integrated terminal.

settings in `devcontainer.json`
```json
"extensions": [
    "ms-python.flake8", // https://marketplace.visualstudio.com/items?itemName=ms-python.flake8
    // ...
],
"settings": {
    //
    // Flake8 - https://marketplace.visualstudio.com/items?itemName=ms-python.flake8
    //
    "flake8.args": [
        "--config",
        "${workspaceFolder}/.devcontainer/.flake8" // default "--config=${workspaceFolder}/.flake8"
    ],
    "flake8.cwd": "${workspaceFolder}", // default ${workspaceFolder}
    "flake8.showNotification": "onError", // 'off' (default), 'onWarning', 'onError', 'always'
    "flake8.enabled": true,
    // ...
},
```

## Mypy
Type checker and type hinting. Automatically runs on <kbd>file save</kbd> action for opened file. Shows its result in code UI and in the `problems` section of VScode integrated terminal.

settings in `devcontainer.json`
```json
"extensions": [
    "ms-python.flake8", // https://marketplace.visualstudio.com/items?itemName=ms-python.flake8
    // ...
],
"settings": {
    //
    // Flake8 - https://marketplace.visualstudio.com/items?itemName=ms-python.flake8
    //
    "flake8.args": [
        "--config",
        "${workspaceFolder}/.devcontainer/.flake8" // default "--config=${workspaceFolder}/.flake8"
    ],
    "flake8.cwd": "${workspaceFolder}", // default ${workspaceFolder}
    "flake8.showNotification": "onError", // 'off' (default), 'onWarning', 'onError', 'always'
    "flake8.enabled": true,
    // ...
},
```

## Bandit
Code scanner of vulnerabilities and code quality. Automatically runs on <kbd>file save</kbd> action for opened file. Shows its result in code UI and in the `problems` section of VScode integrated terminal.

settings in `devcontainer.json`
```json
"extensions": [
    "nwgh.bandit", // https://marketplace.visualstudio.com/items?itemName=nwgh.bandit
    // ...
],
"settings": {
    //
    // Bandit - https://marketplace.visualstudio.com/items?itemName=nwgh.bandit
    //
    "bandit.args": [
        "-c",
        "${workspaceFolder}/.devcontainer/.bandit"
    ],
    "bandit.cwd": "${workspaceFolder}",
    "bandit.logLevel": "warning", // 'error' (default), 'warning'
    "bandit.enabled": true,
    "bandit.showNotification": "onError", // 'off' (default), 'onWarning', 'onError', 'always'
    // ...
},
```
