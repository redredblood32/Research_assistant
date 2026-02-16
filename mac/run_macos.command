#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
cd "${PROJECT_ROOT}"

if [ ! -d ".venv" ]; then
  echo ".venv not found. Run mac/install_macos.sh first."
  exit 1
fi

source .venv/bin/activate
streamlit run Research_assistant_v1.py
