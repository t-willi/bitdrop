#!/usr/bin/env bash
# Mutation check: break the code on purpose and confirm the tests notice.
#
#   scripts/mutation_check.sh <source-file> <tests> '<exact old text>' '<new text>'
#
# Example (the ledger must be what prevents duplicate rows):
#   scripts/mutation_check.sh packages/bitdrop-connector-memory/src/bitdrop_connector_memory/__init__.py \
#       packages/bitdrop-connector-memory/tests '        if entry in self._ledger:' '        if False:'
#
# The source file is restored afterwards, even if the run is interrupted.
# Exit status: 0 = the mutant was KILLED (some test failed: good), 1 = it SURVIVED (all tests
# passed: the tests are too weak), 2 = bad usage or the old text was not found exactly once.
set -u

if [ "$#" -ne 4 ]; then
    sed -n '2,12p' "$0" | sed 's/^# \{0,1\}//'
    exit 2
fi
file="$1"; tests="$2"; old="$3"; new="$4"
cd "$(dirname "$0")/.." || exit 2

backup="$(mktemp)"
cp "$file" "$backup"
trap 'cp "$backup" "$file"; rm -f "$backup"' EXIT

python3 -I - "$file" "$old" "$new" <<'PY' || exit 2
import pathlib, sys
path, old, new = pathlib.Path(sys.argv[1]), sys.argv[2], sys.argv[3]
text = path.read_text()
if text.count(old) != 1:
    sys.exit(f"the old text must occur exactly once in {path}, found {text.count(old)} times")
path.write_text(text.replace(old, new))
PY

output="$(uv run pytest "$tests" -q -p no:cacheprovider 2>&1)"
echo "$output" | grep -E '^(FAILED|ERROR)|passed|failed' | head -n 8
if echo "$output" | grep -qE '[0-9]+ (failed|error)'; then
    echo "mutant KILLED: the tests noticed"
    exit 0
fi
echo "mutant SURVIVED: the tests did not notice this change" >&2
exit 1
