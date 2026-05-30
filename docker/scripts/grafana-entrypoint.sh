#!/bin/sh
set -eu

if [ -z "${GF_SECURITY_ADMIN_PASSWORD:-}" ] || [ "$GF_SECURITY_ADMIN_PASSWORD" = "admin" ]; then
    echo "GF_SECURITY_ADMIN_PASSWORD must be set to a non-default value" >&2
    exit 1
fi

exec /run.sh
