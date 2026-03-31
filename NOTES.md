# PER_OP Hook Infrastructure

Design notes for the PER_OP concurrency control mode, where CC logic is
provided by statically-compiled hook functions in `cc_hooks_*.cpp` files.

## Hook Interface

9 hooks declared in `cc_hooks.h`. All call sites are `if constexpr (cc_alg == CCAlg::PerOp)` guarded.

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

### Placement rationale

- **cc_pre_op in `row_t::get_row()`** — where all other CCs do their row-level action.
- **cc_post_op in `txn_man::get_row()`** — after access recording (`type`, `orig_row` set, `row_cnt` not yet incremented). Has access to full Access struct for copy/metadata decisions.
- **cc_release_op in `row_t::return_row()`** — same cleanup dispatch as all other CCs. Uses XP type convention (WR = committed write, XP = aborted write, RD = read release).
- **cc_pre_commit in `txn_man::finish()`** — alongside `validate_tictoc()`, `validate_silo()`, etc.

## Example Implementations

Four variants, each in `concurrency_control/cc_hooks_*.cpp`:

| Variant | File | Description |
|---------|------|-------------|
| NOOP | `cc_hooks_noop.cpp` | Zero CC overhead; performance upper bound |
| NO_WAIT | `cc_hooks_no_wait.cpp` | NO_WAIT 2PL with `pthread_rwlock` (SH/EX) |
| OCC | `cc_hooks_occ.cpp` | Per-row OCC validation (sort + latch + validate + install) |
| MVCC | `cc_hooks_mvcc.cpp` | Version history with simple GC (prototype) |

Binaries: `rundb_per_op_{noop,2pl,occ,mvcc}_{ycsb,tpcc}`.

## Design Decisions

**D1: Opaque `void* cc_row_state`** — replaces all typed `Row_*` manager pointers. Each CC algorithm casts via file-local `cc_mgr()` helpers. Makes `row.h` CC-neutral.

**D2: `void* cc_txn_state` via `TxnExtra<PerOp>`** — opaque per-txn state. Hook manages lifecycle via `cc_pre_txn` (allocate) / `cc_post_txn` (free).

**D3: cc_post_op owns the copy decision** — infrastructure doesn't pre-allocate `data` for PER_OP. The hook decides whether to work in-place (2PL) or allocate a copy (OCC).

**D4: `op_idx` parameter** — passed through `txn_man::get_row()` → `row_t::get_row()` → `return_row()`. Default `-1` for callers that don't track it.

**D5: No noop fallback** — hook call sites are all constexpr-guarded. Non-PER_OP binaries never reference hook symbols.

**D6: Workload files are hook-free** — `ycsb_txn.cpp` and `tpcc_txn.cpp` have no `#include "cc_hooks.h"`, no CC macros, no hook calls. Just `get_row()` and `finish()`.

## Files Shown to CC Generator

The LLM sees only these files (all CC-neutral):
1. `cc_hooks.h` — hook interface + documented `txn_man`/`row_t` API
2. `row.h` — row struct (just `void* cc_row_state`, no CC types)
3. `ycsb_txn.cpp` / `tpcc_txn.cpp` — transaction logic

The generator does NOT see `txn.h` (contains CC-specific `TxnExtra`/`AccessExtra` specializations for baseline algorithms).

## Limitations

1. **TEST workload incompatible** — calls `txn_man::get_row()` directly, bypassing `cc_pre_op`. Skipped in build and test.

2. **MVCC TPCC too slow** — heavy per-row version history init. Skipped in test.py.

3. **MVCC simplified WAIT** — aborts instead of spinning on pending prewrites.

4. **NOOP infrastructure overhead** — `rdtsc` × 2 per access (stats timing), access array population, cleanup loop iteration. Could be eliminated with PER_OP-specific early-returns but would break design consistency.

## per_op_occ vs Silo Performance Gap

The `per_op_occ` hook implements backward validation structurally similar to
Silo (compare observed row version at commit time), but is ~1.5–1.8× slower
across all thread counts (measured 2026-03-30, TPCC, 1–48 threads).

Root causes (in rough order of impact):

1. **Mutex on every read** — `cc_pre_op` and `cc_post_op` each take
   `pthread_mutex_lock` on the row's latch just to read `wts`. Silo uses a
   lock-free read protocol: load TID word → copy row → re-load TID word →
   retry if changed. No atomic RMW, no syscall.

2. **Latch ALL rows at commit** — `cc_pre_commit` latches every accessed row
   (reads + writes) for validation. Silo only locks the **write set**;
   read-set rows are validated lock-free (check TID unchanged, lock bit clear).

3. **Global `get_ts()` at commit** — calls `glob_manager->get_ts()` twice per
   txn (start + commit). This is a global atomic counter → serialization
   bottleneck. Silo computes `_cur_tid` locally as `max(observed TIDs) + 1` —
   no global coordination. Visible in benchmarks: `time_ts_alloc` is ~1.5 s at
   48 threads for per_op_occ vs 0 for Silo.

4. **malloc/free per access** — `cc_post_op` calls `_mm_malloc` + `row_t::init()`
   for every row access to create a private copy. Silo reuses pre-allocated
   `accesses[]` buffers.

5. **No lock-free TID encoding** — Silo packs a lock bit into `_tid_word`,
   enabling concurrent-writer detection without a mutex. per_op_occ has no
   equivalent; it must hold a mutex to safely read `wts`.

These are not bugs — the per_op_occ hook is intentionally a straightforward
textbook-style OCC. Closing the gap would require adopting Silo's lock-free
read protocol and write-set-only locking, which would essentially reimplement
Silo inside the hook framework.

## Configurable Conflict-Free TPC-C

Runtime flags for disabling cross-warehouse accesses:
- `-Tr FLOAT` — remote customer % in Payment (default 15, TPC-C spec)
- `-Ts FLOAT` — remote supply warehouse % in New-Order (default 1, TPC-C spec)

Conflict-free recipe: `./rundb_per_op_noop_tpcc -t N -n N -Tr0 -Ts0`
