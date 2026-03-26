# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build

```bash
cmake -S . -B build
cmake --build build --parallel
```

This builds one binary per (algorithm, workload) combination: `build/rundb_<alg>_<wl>`.

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
- `-tINT` — thread count
- `-rFLOAT` / `-wFLOAT` — read/write percentage (YCSB)
- `-zFLOAT` — Zipf theta
- `-nINT` — number of warehouses (TPCC)
- `-o FILE` — output file
- `--param=value` — override string params (e.g. `--validation_lock=no-wait`)

## Configuration

Compile-time defaults live in [config.h](config.h). CMake `-D` flags override them; see the `#ifndef` guards at the top of config.h. Key parameters:

- `CC_ALG` — set per binary via `-DDBX_ALGS`; valid values: `NO_WAIT`, `WAIT_DIE`, `DL_DETECT`, `TIMESTAMP`, `MVCC`, `HSTORE`, `OCC`, `TICTOC`, `SILO`, `VLL`, `HEKATON`
- `WORKLOAD` — set per binary via `-DDBX_WORKLOADS`; `YCSB` or `TPCC` (or `TEST`)
- `THREAD_CNT` — number of worker threads (compile-time; set in config.h)
- `INDEX_STRUCT` — `IDX_HASH` or `IDX_BTREE`

## Architecture

DBx1000 is an in-memory OLTP database benchmark for evaluating concurrency control algorithms. It has no client/server split — everything runs in a single process with worker threads.

### Execution flow

`main()` in [system/main.cpp](system/main.cpp):
1. Parse CLI args (`parser.cpp`)
2. Initialize memory allocator, global manager, workload
3. Pre-generate a query queue
4. Spawn `THREAD_CNT` pthreads, each running `thread_t::run()`
5. Each thread pulls queries, creates a `txn_man`, and calls `txn_man::run_txn()`

### Key abstractions

- **`workload`** ([system/wl.h](system/wl.h)) — base class for YCSB/TPCC; initializes tables, indexes, and produces `txn_man` instances
- **`txn_man`** ([system/txn.h](system/txn.h)) — base class for transaction logic; subclassed per workload; calls `row_t::get_row()` / `return_row()` which dispatch to the CC manager
- **`row_t`** ([storage/row.h](storage/row.h)) — a database row; contains a `manager` field whose type is selected at compile time by `CC_ALG`
- **`Row_*` managers** ([concurrency_control/row_*.h](concurrency_control/)) — per-row CC state; one class per algorithm (e.g. `Row_tictoc`, `Row_lock`, `Row_mvcc`)

### CC algorithm selection

The CC algorithm is chosen entirely at compile time via `CC_ALG`. The `row_t::manager` field type, the validation logic in `txn_man`, and the global manager (if any) are all `#if CC_ALG == ...` guarded. Adding a new algorithm requires touching `row.h`, `txn.h`, `txn.cpp`, and creating a new `row_<alg>.h/.cpp` pair.

### Directory structure

- `system/` — core engine: threads, transactions, workload base, global state, stats
- `storage/` — tables, rows, catalog, hash/btree indexes
- `concurrency_control/` — one `.cpp`/`.h` pair per CC algorithm plus per-row managers
- `benchmarks/` — YCSB and TPCC workload implementations and query generators
- `config.h` — all compile-time knobs
- `libs/libjemalloc.a` — bundled jemalloc (used automatically if present)
