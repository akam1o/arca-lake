# Performance baselines

One file per environment label, written by `benchmarks/linux/regress.sh
<label> <out> --record` (or `compare.py --record`). A label names a fixed
set of hosts and a layout (for example `lab-2host`: the two-host layout
of RFC-009 §3 on the validation hosts); a baseline is only ever compared
with runs of the same label, so the thresholds in `compare.py` never
apply to CI runners or other hardware. Functional correctness is checked
by the test suites in CI; performance is checked by this script on the
validation hosts, by hand or on a schedule, and reported in RFC-009.
Baselines contain no host names or addresses (medians only).
