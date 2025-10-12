#!/bin/bash
wheel_path="${1:-$PWD/dist/*.whl}" && echo "Wheel path: $wheel_path"

pip install $wheel_path --force-reinstall
