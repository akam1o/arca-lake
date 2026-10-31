# Benchmark report: <topic>

- Date: YYYY-MM-DD
- arca-lake commit: `<sha>`
- Author:
- Purpose: <which hypothesis or decision this informs; link the DD/OD/RFC>

## Environment (required, complete)

| Item | Value |
| --- | --- |
| Hardware model or cloud instance type | |
| CPU model, core count, SMT | |
| Memory | |
| Storage device class and model (if relevant) | |
| Network (if relevant) | |
| OS and kernel version | |
| Rust toolchain (`rustc -V`) | |
| Relevant library versions | |
| Power / frequency settings, if known | |

If any item cannot be disclosed, **do not publish the report**.

## Method

- Exact command lines.
- Configuration files or parameters.
- Number of runs, warm-up, and how variance was handled.
- Anything that was pinned or isolated (cores, NUMA, cache state).

## Results

Tables with units. Report the statistic criterion reports (median or mean
with confidence interval) and say which.

## Interpretation

What the numbers do and do not show. Separate *Verified* conclusions from
*Hypotheses*. Note threats to validity (background load, thermal throttling,
small sample sizes, single-threaded vs multi-threaded).

## Raw data

Path to the committed raw output (for example an exported criterion
summary), or a statement that the raw data is attached to the pull request.
