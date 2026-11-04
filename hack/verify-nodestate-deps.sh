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

# Evaluate the module as a consumer will get it, not as a local workspace
# resolves it. A go.work overlay can satisfy an import from a sibling
# directory, which would hide a dependency that is external to the published
# module. It also makes "go list -m" print one module per workspace entry,
# and the multi-line result turned the exemption below into a bare
# "^github.com/medik8s/common" that matched every package in the repository.
export GOWORK=off

PACKAGES=(./pkg/nodestate/...)

# Ask which module contains the package under test, rather than which modules
# are in scope. The answer is one line by construction.
if ! MODULE=$(go list -f '{{.Module.Path}}' ./pkg/nodestate); then
  echo "verify-nodestate-deps: cannot resolve the module for ./pkg/nodestate"
  exit 1
fi

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
