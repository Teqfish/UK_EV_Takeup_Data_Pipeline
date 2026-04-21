#!/usr/bin/env bash
set -euo pipefail

KESTRA_URL="${KESTRA_URL:-http://localhost:8082}"
KESTRA_USER="${KESTRA_USER:-kestra@kestra.com}"
KESTRA_PASS="${KESTRA_PASS:-Kestra1234}"

echo "Waiting for Kestra API..."
until curl -sf -u "${KESTRA_USER}:${KESTRA_PASS}" \
  "${KESTRA_URL}/" >/dev/null; do
  sleep 2
done

echo "Deploying flows..."
for f in kestra/flows/*.yml; do
  echo "  -> $f"

  response="$(
    curl -s -u "${KESTRA_USER}:${KESTRA_PASS}" \
      -X POST "${KESTRA_URL}/api/v1/main/flows" \
      -H 'Content-Type: application/x-yaml' \
      --data-binary @"$f"
  )"

  if echo "${response}" | grep -q '"id"'; then
    echo "     created"
  elif echo "${response}" | grep -q 'Flow id already exists'; then
    echo "     already exists"
  else
    echo "Flow deployment failed for ${f}"
    echo "${response}"
    exit 1
  fi
done

echo "Done."
