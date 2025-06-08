# Configuration file for the Sphinx documentation builder.
#
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Project information -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information

# USAGE
# in ./docs/ run `make html``

project = "hello_world"
copyright = "2024, Krijn van der Burg"
author = "Krijn van der Burg"
release = "0.0.1"
# -- General configuration ---------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration

extensions = [
    "sphinx.ext.autodoc",  # Automatically generates documentation from docstrings
    "sphinx.ext.viewcode",  # Adds links to source code
    "sphinx.ext.napoleon",  # Supports Google and NumPy-style docstrings
    "sphinx.ext.todo",  # Tracks TODO items in the documentation
    "autoapi.extension",  # Automatically generates API documentation
]

templates_path = ["_templates"]
exclude_patterns = []


# -- Options for HTML output -------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output

html_theme = "sphinx_rtd_theme"
html_static_path = ["_static"]


# -- Options for autoapi -----------------------------------------------------

autoapi_dirs = ["../../src"]
autoapi_type = "python"
autoapi_keep_files = False

autoapi_options = [
    "members",
    "private-members",
    "undoc-members",
    "show-inheritance",
    "show-module-summary",
    "imported-members",
]
autodoc_typehints = "signature"
