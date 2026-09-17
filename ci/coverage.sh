#!/bin/sh
# Require 100% line coverage of src/ from an lcov tracefile.
#
# clickhouse-c holds its headers to the same bar in tools/coverage.sh, so a
# defensive branch no test can reach belongs in neither repository: rewrite it
# until the reachable behaviour is what the code expresses.
#
# Totals come from the per-line DA records rather than from LF and LH, because
# llvm-cov sums those per generic instantiation: a function reached through one
# type parameter but not another lands in LH as a miss even though every source
# line ran. DA records carry the merged count each report renders.
set -eu
export LC_ALL=C

tracefile=${1:-lcov.info}
outdir=${2:-coverage}
mkdir -p "$outdir"

status=0
awk -F: -v missing="$outdir/missing-lines.txt" '
    BEGIN {
        printf "" > missing
        printf "%-20s %8s %8s %9s\n", "File", "Covered", "Lines", "Coverage"
    }
    /^SF:/ {
        path = substr($0, 4)
        file = path
        sub(/.*\//, "", file)
        lines = 0
        hits = 0
        next
    }
    /^DA:/ {
        split(substr($0, 4), da, ",")
        lines++
        if (da[2] + 0 > 0) hits++
        else print path ":" da[1] >> missing
        next
    }
    /^end_of_record/ {
        printf "%-20s %8d %8d %8.2f%%\n", file, hits, lines,
               lines ? 100 * hits / lines : 0
        total += lines
        covered += hits
    }
    END {
        printf "%-20s %8d %8d %8.2f%%\n", "TOTAL", covered, total,
               total ? 100 * covered / total : 0
        if (!total) {
            print "no coverage data in " ARGV[1]
            exit 2
        }
        if (covered != total) {
            print "require 100% line coverage, see " missing
            exit 2
        }
    }
' "$tracefile" > "$outdir/coverage.txt" || status=$?
cat "$outdir/coverage.txt"
if [ -s "$outdir/missing-lines.txt" ]; then
    echo
    echo "Uncovered lines:"
    sed 's/^/  /' "$outdir/missing-lines.txt"
fi
exit "$status"
