#!/bin/bash
# Apply clang-format to all source files in-place.
#
# Requires Docker (clang-format runs in xianpengshen/clang-tools:22).

set -euo pipefail

DOCKER_IMAGE="xianpengshen/clang-tools:22"

# Collect source files, excluding build dirs and vendored libs
mapfile -t files < <(
    find . \( -name 'build' -o -name 'build_*' -o -name 'libs' \) -prune \
        -o \( -name '*.cpp' -o -name '*.h' \) -print \
    | sort
)

if [ ${#files[@]} -eq 0 ]; then
    echo "No source files found."
    exit 0
fi

echo "Formatting ${#files[@]} files..."

docker run --rm \
    -v "$(pwd):/src" -w /src \
    --user "$(id -u):$(id -g)" \
    "$DOCKER_IMAGE" \
    clang-format -i "${files[@]}"

echo "Done."
