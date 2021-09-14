#!/bin/bash

set -x

NOW_TIMESTAMP="$(date -u +%Y%m%dT%H%M%SZ)"

exec {lock_fd}>/home/user/snapshot.lock || exit 1
flock -n "$lock_fd" || { echo "ERROR: flock() failed." >&2; exit 1; }

pg_dump postgresql://snapshot:snapshot@127.0.0.1/snapshot \
    > "/usr/local/debian/snapshot-${NOW_TIMESTAMP}.psql"

flock -u "$lock_fd"
