@echo off
setlocal

set "SCRIPT_DIR=%~dp0"
cd /d "%SCRIPT_DIR%.."

if not exist ".venv" (
  echo .venv not found. Run windows\\install_win.bat first.
  exit /b 1
)

call .venv\Scripts\activate.bat
streamlit run Research_assistant_v1.py
