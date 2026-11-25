#!/usr/bin/env bash
set -euo pipefail
demo_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
if [[ "${1:-}" == --background ]]; then
    shift
    umask 077
    log="$(mktemp "$demo_dir/logs/flow1.XXXXXX.log")"
    nohup "$demo_dir/../flow1-python/.venv/bin/python" "$demo_dir/demo_runner.py" "$@" \
        </dev/null >"$log" 2>&1 &
    printf 'PID %s\nLog %s\n' "$!" "$log"
    exit 0
fi
exec "$demo_dir/../flow1-python/.venv/bin/python" "$demo_dir/demo_runner.py" "$@"
