#!/usr/bin/env bash
# The static checks CI's `checks` job runs; the suite stays outside (run targeted: uv run pytest <files>).
set -euo pipefail
cd "$(dirname "$0")/.."

step() {
  printf '\n== %s ==\n' "$1"
  shift
  "$@"
}

step "Ruff" uv run --locked ruff check src tests
step "Format" uv run --locked ruff format --check src tests
step "Declared dependencies" uv run --locked deptry src
step "Pyright" uv run --locked pyright --stats
step "Public types are fully annotated" uv run --locked pyright --verifytypes tandem --ignoreexternal
step "Key literals stay in tandem.keys" python3 scripts/check_key_literals.py
step "Import contracts" uv run --locked lint-imports
