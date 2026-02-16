#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
cd "${PROJECT_ROOT}"

python3 -m venv .venv

source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -r requirements.txt

echo "Virtual environment created in .venv and requirements installed."
