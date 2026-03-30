# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build

```bash
cmake -S . -B build
cmake --build build --parallel
```

This builds one binary per (algorithm, workload) combination: `build/rundb_<alg>_<wl>`.
PER_OP hook variants produce `build/rundb_per_op_<variant>_<wl>` (e.g., `rundb_per_op_2pl_ycsb`).

Default build type is `RelWithDebInfo`. To change: `cmake -S . -B build -DCMAKE_BUILD_TYPE=Debug`

Build flags: `-Wall -Werror` — all warnings are treated as errors.

### CMake options

- `-DDBX_TEST_MODE=ON` — use smaller table sizes for faster testing

To change which algorithms or workloads are built, edit `DBX_ALGS` / `DBX_WORKLOADS` in `CMakeLists.txt`.

### Running tests

```bash
python3 test.py
```

## Run

```bash
./build/rundb_<alg>_<wl> [options]
```

Example: `./build/rundb_tictoc_ycsb -t8`

Key runtime flags:
- `-tINT` — thread count (overrides compile-time `THREAD_CNT` default)
- `-rFLOAT` / `-wFLOAT` — read/write percentage (YCSB)
- `-zFLOAT` — Zipf theta
- `-nINT` — number of warehouses (TPCC)
- `-o FILE` — output file
- `--param=value` — override string params (e.g. `--validation_lock=no-wait`)

## Configuration

Compile-time defaults live in [config.h](config.h). CMake `-D` flags override them; see the `#ifndef` guards at the top of config.h. Key parameters:

- `CC_ALG` — set per binary via `-DDBX_ALGS`; valid values: `NO_WAIT`, `WAIT_DIE`, `DL_DETECT`, `TIMESTAMP`, `MVCC`, `HSTORE`, `OCC`, `TICTOC`, `SILO`, `VLL`, `HEKATON`, `PER_OP`
- `WORKLOAD` — set per binary via `-DDBX_WORKLOADS`; `YCSB` or `TPCC` (or `TEST`)
- `THREAD_CNT` — compile-time default for thread count; overridden at runtime by `-t` (stored in `g_thread_cnt`)
- `INDEX_STRUCT` — `IDX_HASH` or `IDX_BTREE`

## Architecture

DBx1000 is an in-memory OLTP database benchmark for evaluating concurrency control algorithms. It has no client/server split — everything runs in a single process with worker threads.

### Execution flow

`main()` in [system/main.cpp](system/main.cpp):
1. Parse CLI args (`parser.cpp`)
2. Initialize memory allocator, global manager, workload
3. Pre-generate a query queue
4. Spawn `g_thread_cnt` pthreads, each running `worker_thread_entry()` → `thread_t::run()`
5. Each thread holds one `txn_man` instance (created once at startup, reused across transactions) and calls `run_txn()` per query

### Key abstractions

- **`workload`** ([system/wl.h](system/wl.h)) — base class for YCSB/TPCC; initializes tables, indexes, and produces `txn_man` instances
- **`txn_man`** ([system/txn.h](system/txn.h)) — base class for transaction logic; subclassed per workload; calls `row_t::get_row()` / `return_row()` which dispatch to the CC manager. `cleanup()` resets all per-transaction state at commit/abort.
- **`row_t`** ([storage/row.h](storage/row.h)) — a database row; contains `void* cc_row_state` (opaque per-row CC state, cast by each algorithm's `cc_mgr()` helper)
- **`Row_*` managers** ([concurrency_control/row_*.h](concurrency_control/)) — per-row CC state; one class per algorithm (e.g. `Row_tictoc`, `Row_lock`, `Row_mvcc`). Accessed via `cc_mgr(row)` cast helpers, not directly through `row_t`.

### CC algorithm selection

The CC algorithm is chosen entirely at compile time via `CC_ALG`. The `row_t::cc_row_state` cast helpers, the validation logic in `txn_man`, and the global manager (if any) are all `#if CC_ALG == ...` guarded. Adding a new baseline algorithm requires touching `row.cpp`, `txn.h`, `txn.cpp`, and creating a new `row_<alg>.h/.cpp` pair. For PER_OP, just add a new `cc_hooks_<variant>.cpp` and register it in `DBX_PER_OP_VARIANTS`.

### Critical build constraint: CC_ALG affects txn_man struct layout

Some CC algorithms add conditional fields to `txn_man` (e.g. HEKATON adds `void * volatile history_entry`). This shifts the offsets of all subsequent members. Any source file that includes `txn.h` — directly or indirectly — **must** be compiled with the correct `CC_ALG` definition, or struct member accesses will silently hit wrong offsets (ODR violation → memory corruption).

This is why the CMake build compiles all benchmark files (`*_txn.cpp`, `*_wl.cpp`) as per-algorithm OBJECT libraries (`benchmarks_${wl}_${alg}`) with both `CC_ALG` and `WORKLOAD` defined. Do not move workload source files into a shared library that lacks `CC_ALG`.

### Directory structure

- `system/` — core engine: threads, transactions, workload base, global state, stats
- `storage/` — tables, rows, catalog, hash/btree indexes
- `concurrency_control/` — one `.cpp`/`.h` pair per CC algorithm plus per-row managers
- `benchmarks/` — YCSB and TPCC workload implementations and query generators
- `config.h` — all compile-time knobs
- `libs/libjemalloc.a` — bundled jemalloc (used automatically if present)

## What not to touch

**CC algorithm implementations** (`concurrency_control/row_*.cpp/h`, `silo.cpp`, `tictoc.cpp`, etc.) — these work as-is; the risk of cleanup outweighs the benefit for a research prototype.

## In-progress / known gaps

- **TPCC**: only `payment` and `new_order` are implemented. `order_status`, `delivery`, and `stock_level` are planned. Do not remove their stubs (`run_order_status`, `run_delivery`, `run_stock_level` in `tpcc_txn.cpp`, related index/table members in `tpcc.h`, or the `threadInitXxx` declarations for the corresponding table-init threads).
- **YCSB row data**: field 0 is intended to store the primary key, but the current initializer loop overwrites it with `"hello"`. See the TODO comment in `ycsb_wl.cpp:init_table_slice()`. Fix requires: (1) use `set_value(0, &primary_key, sizeof(primary_key))` or widen the F0 field to 8 bytes in `YCSB_schema.txt`; (2) start the "hello" loop at `fid = 1`.
