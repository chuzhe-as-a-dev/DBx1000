# CLAUDE.md

Guidance for Claude Code when working with this repository.
See [README.md](README.md) for build/run/configuration reference.

## Quick Reference

```bash
cmake -S . -B build && cmake --build build --parallel "$(nproc 2>/dev/null || sysctl -n hw.ncpu)"   # build
python3 test.py                                          # test
./build/rundb_tictoc_ycsb -t8                            # run
```

Build flags: `-Wall -Werror`. Default type: `RelWithDebInfo`.
Test mode: `cmake -S . -B build -DDBX_TEST_MODE=ON`

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

## What not to touch

**CC algorithm implementations** (`concurrency_control/row_*.cpp/h`, `silo.cpp`, `tictoc.cpp`, etc.) — these work as-is; the risk of cleanup outweighs the benefit.

## In-progress / known gaps

- **TPCC**: only `payment` and `new_order` are implemented. Do not remove stubs for `order_status`, `delivery`, `stock_level` (in `tpcc_txn.cpp` and `tpcc.h`).
- **YCSB row data**: field 0 should store the primary key but is overwritten with `"hello"`. See TODO in `ycsb_wl.cpp:init_table_slice()`.
- **Baseline OCC + TPCC**: hangs during table initialization. Not included in `DBX_ALGS`.
