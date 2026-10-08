#!/bin/zsh
set -e
task_root="$(cd "$(dirname "$0")/../.." && pwd)"
task_python="$HOME/.cache/codex-runtimes/codex-primary-runtime/dependencies/python/bin/python3"
if [[ ! -x "$task_python" ]]; then task_python="python3"; fi
exec "$task_python" "$task_root/scripts/guam_onbook/import_onbook.py" --config "$task_root/local/guam-onbook/config.json" "$@"
