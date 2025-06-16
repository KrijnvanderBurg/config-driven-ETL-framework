"""Built-in transform functions for data manipulation.

This module contains concrete implementations of transformation functions
that can be used in the ingestion framework to manipulate data.

Each transform function is designed to perform a specific data operation
and is automatically registered with the TransformFunctionRegistry when imported.
This allows the functions to be referenced by name in configuration files.
"""

import importlib
import pkgutil
from pathlib import Path

# Automatically import all Python modules in this package (transforms directory)
# This will register all transforms without requiring explicit imports
__path__ = [str(Path(__file__).parent)]
for _, module_name, _ in pkgutil.iter_modules(__path__):
    if module_name != "__init__":
        importlib.import_module(f"{__name__}.{module_name}")
