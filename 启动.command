#!/bin/zsh
cd -- "$(dirname -- "$0")" || exit 1
exec ../../work/telegram-workbench-runtime/venv/bin/python -B launch.py
