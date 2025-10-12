#!/bin/bash
target_path="${1:-$PWD/src}" && echo "Scanning folder: $target_path"

# Install graudit if not already installed
if ! command -v graudit &> /dev/null; then
    echo "Installing graudit..."
    git clone https://github.com/wireghoul/graudit $HOME/.graudit/
    echo "export PATH=$PATH:$HOME/.graudit" >> ~/.profile
    . ~/.profile
fi
echo -n "graudit version: " && graudit -v

graudit -d python -A "$target_path"
