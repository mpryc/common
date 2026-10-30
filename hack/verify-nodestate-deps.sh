#!/bin/bash
#
# pkg/nodestate must import nothing outside the Go standard library.
#
# Two operators on different Kubernetes versions consume it, and it is on the
# critical path of both. A dependency here — even k8s.io/apimachinery — would
# have to be reconciled across every consumer before any of them could take a
# fix. The state file is JSON on disk and needs none of it.
#
# A package path whose first element contains a dot is a module path, and so
# is external; everything else is standard library or internal to it.

set -euo pipefail

PACKAGES=(./pkg/nodestate/...)
MODULE=$(go list -m)

external=$(
  go list -deps "${PACKAGES[@]}" |
    awk -F/ '$1 ~ /\./ { print }' |
    grep -v "^${MODULE}\(/\|$\)" || true
)

if [ -n "$external" ]; then
  echo "pkg/nodestate must depend only on the standard library, but it imports:"
  echo "$external" | sed 's/^/  /'
  exit 1
fi

echo "pkg/nodestate depends only on the standard library."
