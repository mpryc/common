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

# Resolve the dependency list first. Folding this into the filtering pipeline
# would let a build failure arrive as an empty list, which the filter then
# reports as "no external dependencies" — the gate passing precisely when it
# could not run.
if ! deps=$(go list -deps "${PACKAGES[@]}"); then
  echo "verify-nodestate-deps: go list failed; the dependency check did not run"
  exit 1
fi

# Exempt only this package and its subpackages. Exempting the whole module
# would let pkg/nodestate import another package from this repository and
# still pass, which the package documentation says it does not do.
external=$(
  printf '%s\n' "$deps" |
    awk -F/ '$1 ~ /\./ { print }' |
    grep -v "^${MODULE}/pkg/nodestate\(/\|$\)" || true
)

if [ -n "$external" ]; then
  echo "pkg/nodestate must depend only on the standard library, but it imports:"
  echo "$external" | sed 's/^/  /'
  exit 1
fi

echo "pkg/nodestate depends only on the standard library."
