# PER_OP CC Hook Infrastructure — Implementation Notes

## Current Hook Interface (`cc_hooks.h`)

9 hooks, all `#if CC_ALG == PER_OP` guarded at call sites:

| Hook | Called from | Purpose |
|------|-----------|---------|
| `cc_init_row_state` | `row.cpp::init_manager()` | Allocate per-row CC state |
| `cc_free_row_state` | (not yet called) | Free per-row CC state |
| `cc_pre_txn` | `thread.cpp::run()` | Per-txn setup (ts alloc, etc.) |
| `cc_post_txn` | `thread.cpp::run()` | Per-txn teardown |
| `cc_pre_op` | `row.cpp::get_row()` | Lock/version check before access |
| `cc_post_op` | `txn.cpp::get_row()` | Copy/snapshot decision, access metadata |
| `cc_pre_commit` | `txn.cpp::finish()` | Commit-time validation |
| `cc_release_op` | `row.cpp::return_row()` | Release locks, free copies |
| `cc_global_init` | `main.cpp` | One-time global CC init |

## Hook Placement Rationale

- **cc_pre_op in `row_t::get_row()`**: Row-level CC action (lock, version check) — same dispatch point as all other CC algorithms.
- **cc_post_op in `txn_man::get_row()`**: Called after access recording (`type`, `orig_row` set), before `row_cnt++`. Has access to the full Access struct via `txn->accesses[txn->row_cnt]`. Can set `data`, `orig_data`, or any per-access metadata.
- **cc_release_op in `row_t::return_row()`**: Same cleanup dispatch point as all other CCs. No `rc` parameter — uses XP type convention (`type == WR` = committed write, `type == XP` = aborted write, `type == RD` = read release).
- **cc_pre_commit in `txn_man::finish()`**: Alongside `validate_tictoc()`, `validate_silo()`, etc.

## Build System

Binary naming: `rundb_per_op_{variant}_{workload}` (e.g., `rundb_per_op_2pl_ycsb`).

Three hook variants, each in its own file:
- `cc_hooks_2pl.cpp` — NO_WAIT 2PL (pthread_mutex trylock)
- `cc_hooks_occ.cpp` — OCC per-row validation (sort + latch + validate + install)
- `cc_hooks_mvcc.cpp` — MVCC version history (prototype; simplified WAIT handling)

CMake builds per-variant object libraries (`cc_per_op_2pl`, `cc_per_op_occ`, `cc_per_op_mvcc`), each compiled with `CC_ALG=PER_OP`. Non-PER_OP algorithms don't compile any hook file.

## Key Design Decisions

### D1: Opaque `void* cc_row_state` for ALL algorithms
Replaced all typed `Row_*` manager pointers in `row.h` with a single `void* cc_row_state`. Each CC algorithm casts via a file-local `cc_mgr()` helper. Makes `row.h` completely CC-neutral.

### D2: `void* cc_txn_state` on txn_man
Under `#if CC_ALG == PER_OP`, provides opaque per-transaction state. Hook manages lifecycle via `cc_pre_txn` (allocate) and `cc_post_txn` (free).

### D3: cc_post_op owns the copy decision
The copy/snapshot strategy is entirely in cc_post_op. 2PL: noop (write in-place under lock). OCC: allocate copy, record observed wts. Infrastructure doesn't pre-allocate `data` or `orig_data` for PER_OP.

### D4: `op_idx` = running `get_row()` call counter
Passed through `txn_man::get_row(row, type, op_idx)` → `row_t::get_row(type, txn, row, op_idx)`. Default `-1` for callers that don't track it (test_txn.cpp, tpcc_txn.cpp).

### D5: ycsb_txn.cpp is zero-hook, zero-macro
No `#include "cc_hooks.h"`, no CC_ALG macros, no hook calls. Just `get_row(row, type, op_cnt)` and `finish(rc)`. The only preprocessor directive is `#if INDEX_STRUCT == IDX_BTREE`.

### D6: No noop fallback
Hook call sites are all `#if CC_ALG == PER_OP` guarded. Non-PER_OP algorithms never reference hook symbols. No noop definitions needed.

## Limitations

1. **TEST workload incompatible**: Calls `txn_man::get_row()` directly from `test_txn.cpp`, bypassing `cc_pre_op` in `row_t::get_row()`. Skipped in build (CMake) and test (test.py).

2. **MVCC TPCC too slow**: Heavy per-row version history init (`_mm_malloc` per row × millions of TPCC rows). Skipped in test.py.

3. **MVCC simplified WAIT**: Aborts instead of spinning on pending prewrites. Works for low-contention YCSB but may cause liveness issues under high write contention.

4. **HSTORE incompatibility**: Partition-level locks don't fit the per-row hook model.

5. **OCC sort reorders accesses[]**: `cc_pre_commit` sorts by key order, shifting op_idx-keyed parallel arrays. OCC hooks swap `observed_wts[]` in sync.

6. **NOOP still has non-trivial infrastructure overhead** compared to HSTORE (which early-returns from `get_row()` and `cleanup()`). PER_OP NOOP still pays:
   - `rdtsc` × 2 per access in `txn_man::get_row()` (stats timing, ~30-50ns each)
   - Access array population (`type`, `orig_row`) per access
   - Cleanup loop iterating all accesses, calling `return_row` → `cc_release_op` (noop) per access
   - `rdtsc` × 2 per transaction in `finish()`
   These could be eliminated with PER_OP-specific early-returns in `get_row()` and `cleanup()`, but that would re-introduce the bypass logic we removed for design consistency.

## Workflow for LLM Generation

1. Show LLM: `cc_hooks.h` (interface), `ycsb_txn.cpp` (transaction logic), `row.h` (row struct)
2. LLM generates a new `cc_hooks_<variant>.cpp`
3. Add variant to `DBX_PER_OP_VARIANTS` in `CMakeLists.txt`, rebuild, run
4. Compare against baselines
