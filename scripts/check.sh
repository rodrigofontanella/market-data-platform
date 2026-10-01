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
EXPECTED_CHECKS=7

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

run "ruff" .venv/bin/ruff check .

PYTEST=".venv/bin/pytest"
COVERAGE=".venv/bin/coverage"

if [[ ! -x "${PYTEST}" ]]; then
  echo "no ${PYTEST}. run: python3 -m venv .venv && .venv/bin/pip install -r requirements-dev.txt" >&2
  exit 1
fi

# Coverage. Every suite runs under `coverage run`, all of them write parallel
# data files to the repository root, and the "coverage" check combines them.
# Both paths must be absolute: each suite cd's into its own directory first.
export COVERAGE_RCFILE="${PWD}/.coveragerc"
export COVERAGE_FILE="${PWD}/.coverage"
# Leftovers from an earlier run that died before combining would otherwise be
# merged into this run's numbers.
rm -f .coverage .coverage.*

run "market_core" bash -c 'cd libs/market_core && ../../.venv/bin/coverage run -m pytest -q'

run "producer" bash -c 'cd apps/producer && ../../.venv/bin/coverage run -m pytest -q'

if [[ "${ALL}" == true ]]; then
  run "consumer" bash -c 'cd apps/consumer && ../../.venv/bin/coverage run -m pytest -q'
else
  run "consumer" bash -c 'cd apps/consumer && ../../.venv/bin/coverage run -m pytest -q -m "not integration"'
fi


run "alert rules" docker run --rm \
  -v "${PWD}/docker/prometheus/rules:/rules:ro" \
  --entrypoint promtool prom/prometheus \
  test rules /rules/market-data.rules.test.yml


if [[ "${ALL}" == true ]]; then
  run "api" bash -c 'cd apps/api && ../../.venv/bin/coverage run -m pytest -q'
else
  run "api" bash -c 'cd apps/api && ../../.venv/bin/coverage run -m pytest -q -m "not integration"'
fi

# Every source root must have data. A suite that ran without coverage, or whose
# data never reached the root, would otherwise leave a total that looks
# plausible and covers less than it claims.
COVERAGE_ROOTS=(
  apps/api/app
  apps/consumer/app
  apps/producer/app
  libs/market_core/src/market_core
  libs/market_testing/src/market_testing
)

coverage_report() {
  "${COVERAGE}" combine --quiet || return 1
  "${COVERAGE}" report || return 1
  local root missing=0
  for root in "${COVERAGE_ROOTS[@]}"; do
    if ! "${COVERAGE}" report --include="${root}/*" >/dev/null 2>&1; then
      echo "no coverage data under ${root}" >&2
      missing=1
    fi
  done
  return "${missing}"
}

run "coverage" coverage_report

if [[ ${RAN} -ne ${EXPECTED_CHECKS} ]]; then
  echo "expected ${EXPECTED_CHECKS} checks, ran ${RAN}" >&2
  exit 1
fi

if [[ ${#FAILED[@]} -gt 0 ]]; then
  echo "FAILED: ${FAILED[*]}"
  exit 1
fi

echo "all checks passed"

