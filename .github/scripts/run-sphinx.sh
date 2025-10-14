#!/bin/bash
docs_dirpath="${1:-$PWD/docs}" && echo "Set CWD to: $docs_dirpath"

# Install sphinx if not already installed
if ! command -v sphinx-build &> /dev/null; then
    echo "Installing sphinx..."
    pip install sphinx --quiet
    pip install sphinx-autoapi sphinx-rtd-theme --quiet
fi

cd $docs_dirpath

make html
