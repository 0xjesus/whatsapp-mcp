#!/bin/sh
# Set up only Python dependencies. PostgreSQL and model servers are separate.
set -eu
umask 077
package_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd -P)
venv_dir=${1:-"$HOME/.local/share/messaging-memory/venv"}
case "$venv_dir" in
  /*) ;;
  *) printf '%s\n' 'Pass an absolute virtual environment path.' >&2; exit 1 ;;
esac
if ! command -v uv >/dev/null 2>&1; then
  printf '%s\n' 'Install uv and add its executable directory to PATH first.' >&2
  exit 1
fi
uv venv --python '>=3.10' "$venv_dir"
uv pip sync --python "$venv_dir/bin/python" "$package_dir/requirements.lock"
printf 'Python ready: %s\nLauncher: %s\n' "$venv_dir/bin/python" "$package_dir/history.py"
