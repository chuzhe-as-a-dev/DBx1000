#!/bin/bash
# Check all source files against .clang-format without modifying them.
# Exits 0 if all files are correctly formatted, 1 if any need changes.
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

echo "Checking ${#files[@]} files..."

if docker run --rm \
    -v "$(pwd):/src" -w /src \
    "$DOCKER_IMAGE" \
    clang-format --dry-run --Werror "${files[@]}"; then
    echo "OK: all files are correctly formatted."
    exit 0
else
    echo ""
    echo "FAIL: the files listed above need formatting."
    echo "Run format.sh to apply fixes."
    exit 1
fi
