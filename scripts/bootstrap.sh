#!/usr/bin/env bash
#
# Build .venv at the repository root: the environment ./scripts/check.sh runs in.
#
# This is the one definition of the install sequence. CI runs it, and so does
# anyone setting up a checkout. It does not touch Docker, .env or any image.
#
# Stops at the first failure (set -e), unlike check.sh, which collects them:
# every step here needs the one before it, and a half-built venv followed by
# "done" is the worst possible outcome.
#
# Every run starts from an empty venv (--clear). Installing on top of an
# existing one is how a hand-installed package hides: pip leaves satisfied
# requirements alone, so anything added by hand survives every re-run.
#
# One pip invocation resolves the pinned files and the editable libs together,
# so the libs' >= floors are satisfied by the pins and install order does not
# matter. Two files pinning different versions of one package fail here as a
# resolution error, instead of a later install silently replacing an earlier
# one's version.

set -euo pipefail

case "${1:-}" in
  "") ;;
  *)  echo "usage: $(basename "$0")" >&2; exit 2 ;;
esac

cd "$(dirname "${BASH_SOURCE[0]}")/.." || exit 1

# The production image is python:3.12-slim (docker/Dockerfile.python). A venv on
# any other minor version tests an interpreter production never runs.
REQUIRED_PYTHON="3.12"
PYTHON="${PYTHON:-python3}"

if ! command -v "${PYTHON}" >/dev/null 2>&1; then
  echo "bootstrap: '${PYTHON}' not found" >&2
  exit 1
fi

actual="$("${PYTHON}" -c 'import sys; print("%d.%d" % sys.version_info[:2])')"
if [[ "${actual}" != "${REQUIRED_PYTHON}" ]]; then
  echo "bootstrap: need Python ${REQUIRED_PYTHON}, '${PYTHON}' is ${actual}" >&2
  echo "bootstrap: set PYTHON=/path/to/python${REQUIRED_PYTHON} to use another" >&2
  exit 1
fi

echo "=== venv: .venv from ${PYTHON} (${actual}), cleared"
"${PYTHON}" -m venv --clear .venv

echo "=== pip install: four pinned files + two editable libs, one resolution"
.venv/bin/pip install --disable-pip-version-check \
  -r requirements-dev.txt \
  -r apps/api/requirements.txt \
  -r apps/consumer/requirements.txt \
  -r apps/producer/requirements.txt \
  -e libs/market_core \
  -e libs/market_testing

echo "=== pip check"
.venv/bin/pip check

echo "bootstrap: .venv ready. next: ./scripts/check.sh [--all]"