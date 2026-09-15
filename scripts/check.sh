#!/usr/bin/env bash

set -uo pipefail

ALL=false
case "${1:-}" in
  --all) ALL=true ;;
  "")    ;;
  *)     echo "usage: $(basename "$0") [--all]" >&2; exit 2 ;;
esac

cd "$(dirname "${BASH_SOURCE[0]}")/.." || exit 1

FAILED=()
RAN=0
EXPECTED_CHECKS=4

run() {
  local label="$1"; shift
  RAN=$((RAN + 1))
  echo "=== ${label}"
  if "$@"; then
    echo "--- ${label}: ok"
  else
    FAILED+=("${label}")
    echo "--- ${label}: FAILED"
  fi
  
}


PYTEST=".venv/bin/pytest"
if [[ ! -x "${PYTEST}" ]]; then
  echo "no ${PYTEST}. run: python3 -m venv .venv && .venv/bin/pip install -r requirements-dev.txt" >&2
  exit 1
fi

run "market_core" bash -c 'cd libs/market_core && ../../.venv/bin/pytest -q'

run "producer" bash -c 'cd apps/producer && ../../.venv/bin/pytest -q'

if [[ "${ALL}" == true ]]; then
  run "consumer" bash -c 'cd apps/consumer && ../../.venv/bin/pytest -q'
else
  run "consumer" bash -c 'cd apps/consumer && ../../.venv/bin/pytest -q -m "not integration"'
fi


run "alert rules" docker run --rm \
  -v "${PWD}/docker/prometheus/rules:/rules:ro" \
  --entrypoint promtool prom/prometheus \
  test rules /rules/market-data.rules.test.yml

if [[ ${RAN} -ne ${EXPECTED_CHECKS} ]]; then
  echo "expected ${EXPECTED_CHECKS} checks, ran ${RAN}" >&2
  exit 1
fi

if [[ "${ALL}" == true ]]; then
  run "api" bash -c 'cd apps/api && ../../.venv/bin/pytest -q'
else
  run "api" bash -c 'cd apps/api && ../../.venv/bin/pytest -q -m "not integration"'
fi


if [[ ${#FAILED[@]} -gt 0 ]]; then
  echo "FAILED: ${FAILED[*]}"
  exit 1
fi

echo "all checks passed"

