# CLAUDE.md

Guidance for Claude Code when working with this repository.
See [README.md](README.md) for build/run/configuration reference.

## Quick Reference

```bash
cmake -S . -B build && cmake --build build --parallel "$(nproc 2>/dev/null || sysctl -n hw.ncpu)"   # build
python3 test.py                                          # test
./build/rundb_tictoc_ycsb -t8                            # run
./format.sh                                              # format (required before every commit)
python3 bench.py --algs silo,tictoc --threads 1,4,16     # perf benchmark
```

Build flags: `-Wall -Werror`. Default type: `RelWithDebInfo`.
Test mode: `cmake -S . -B build -DDBX_TEST_MODE=ON`

**Always run `./format.sh` before committing.** It applies clang-format (via
Docker) to all `*.cpp`/`*.h` files. Use `./format-check.sh` to verify without
modifying — CI fails if anything is unformatted.

## Architecture

DBx1000 is an in-memory OLTP benchmark — single process, worker threads, no client/server split.

### Execution flow

`main()` → parse args → init allocator/workload → pre-generate queries → spawn threads → each thread runs `txn_man::run_txn()` per query.

### Key abstractions

- **`workload`** ([system/wl.h](system/wl.h)) — base class for YCSB/TPCC; initializes tables and indexes
- **`txn_man`** ([system/txn.h](system/txn.h)) — transaction logic; calls `row_t::get_row()` / `return_row()` for CC dispatch. `cleanup()` releases all accessed rows.
- **`row_t`** ([storage/row.h](storage/row.h)) — a database row; contains `void* cc_row_state` (opaque, cast by each CC algorithm's `cc_mgr()` helper)
- **`Row_*` managers** ([concurrency_control/row_*.h](concurrency_control/)) — per-row CC state; accessed via `cc_mgr(row)` cast helpers

### Compile-time dispatch

CC algorithm selected by `CC_ALG` macro. Uses C++23 `enum class CCAlg` + `if constexpr` dispatch:

- **`if constexpr (cc_alg == CCAlg::Tictoc)`** — behavioral dispatch
- **Template lambdas** `[&]<CCAlg A = cc_alg>() { ... }()` — for branches with different manager APIs (makes names dependent so false branches are discarded)
- **`TxnExtra<CCAlg>` / `AccessExtra<CCAlg>`** — per-algorithm fields via template specialization, inherited by `txn_man` / `Access`
- **`RowManagerType<CCAlg>`** — type trait mapping algorithm → `Row_*` class

Old `#define` macros remain in `config.h` for backward compatibility.

### Build constraint: CC_ALG affects struct layout

`TxnExtra` specializations add different fields to `txn_man` per algorithm. Every file including `txn.h` must compile with the correct `CC_ALG`. This is why CMake builds per-algorithm OBJECT libraries. Only CC-independent files are in `_common` libraries.

### PER_OP hooks

See [NOTES.md](NOTES.md) for the PER_OP hook infrastructure design. To add a new hook variant: create `cc_hooks_<name>.cpp`, add to `DBX_PER_OP_VARIANTS` in `CMakeLists.txt`.

Current variants: `NOOP`, `NO_WAIT`, `OCC`, `MVCC`, `PJ_TPCC_4WH` (Polyjuice
port — learned CC for TPCC 4-warehouse, see
[concurrency_control/cc_hooks_pj_tpcc_4wh.cpp](concurrency_control/cc_hooks_pj_tpcc_4wh.cpp)).

### Polyjuice-ordered TPCC (`tpcc_txn_pj.cpp`)

The learned policy in `pj_tpcc_4wh` was trained against Polyjuice's specific
TPCC op sequence, which differs slightly from DBx1000's standard
`tpcc_txn.cpp` (customer moved to end of new_order; items and stocks in
separate loops). `benchmarks/tpcc_txn_pj.cpp` provides that ordering.

For each CC algorithm in `DBX_ALGS`, two TPCC binaries are produced:
- `rundb_<alg>_tpcc`      — standard TPCC (`tpcc_txn.cpp`)
- `rundb_<alg>_tpcc_pj`   — Polyjuice-ordered TPCC (`tpcc_txn_pj.cpp`)

This lets every baseline algorithm be measured against the exact same TPCC
code that the Polyjuice variant runs. `bench.py --workload tpcc_pj` targets
the `_pj` binaries.

## What not to touch

**CC algorithm implementations** (`concurrency_control/row_*.cpp/h`, `silo.cpp`, `tictoc.cpp`, etc.) — these work as-is; the risk of cleanup outweighs the benefit.

## In-progress / known gaps

- **TPCC**: only `payment` and `new_order` are implemented. Do not remove stubs for `order_status`, `delivery`, `stock_level` (in `tpcc_txn.cpp` and `tpcc.h`).
- **YCSB row data**: field 0 should store the primary key but is overwritten with `"hello"`. See TODO in `ycsb_wl.cpp:init_table_slice()`.
- **Baseline OCC + TPCC**: hangs during table initialization. Not included in `DBX_ALGS`.
- **`CPU_FREQ`** in `config.h` is hardcoded to 2 GHz; `get_sys_clock()` converts RDTSC to ns using that constant, so reported timings are off by up to ~15% on other machines (no functional impact — throughput is still measured from a separate wall-clock).
- **`pj_tpcc_4wh` learned backoff** is disabled by default; the Polyjuice-trained multipliers over-wait on DBx1000's tighter inner loop (measured 2× throughput loss at 8-24 threads). Enable with `-DPJ_ENABLE_BACKOFF=1` for comparison.
