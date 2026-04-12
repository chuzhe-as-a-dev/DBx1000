// cc_hooks_pj_tpcc_4wh.cpp — Faithful Polyjuice learned policy.
// Dirty reads, dependency tracking, piece boundaries, atomic final commit.
// orig_row NEVER modified until final commit. Writes go to local copies.
//
// Serializability correctness argument:
//   - Clean reads: Silo-style OCC. Record tid at read time; at commit,
//     lock write rows (sorted), validate all read tids unchanged, compute
//     commit_tid > all observed tids, install writes, unlock.
//   - Dirty reads: dependency tracking. Reader adds dep on writer; at
//     pre_commit, waits for dep to finish. If writer aborted → cascade
//     abort. If writer committed → validate row.tid_word == writer's
//     commit_tid (no intervening writes). If stale → abort.
//   - Write-write ordering: later writer records w-w dep on earlier
//     uncommitted writer seen in dirty_head during piece validation.
//     Waits for earlier writer to finish before committing.
//   - Piece validation: early detection of stale reads within a piece.
//     Exposes PUBLIC writes to dirty_head for pipelining. Does NOT
//     advance tid_word (only final_commit does).
//
// Correctness assumptions (not enforced, must be maintained by workload):
//   1. No read-your-own-writes: a txn must not re-read a row it has
//      written (tpcc_txn_pj.cpp does not do this).
//   2. Same row not written in multiple pieces by the same txn (dirty_head
//      cleanup in final_commit removes only the first matching entry).
//   3. No phantom protection (range queries not handled; TPCC uses only
//      primary-key lookups).

#include <mm_malloc.h>

#include <algorithm>
#include <array>
#include <cassert>
#include <cstdlib>
#include <cstring>

#include "cc_hooks.h"
#include "manager.h"
#include "row.h"
#include "thread.h"
#include "tpcc_query.h"
#include "txn.h"

// ---- Policy table (48th-4wh.txt) ----
// Each entry corresponds to one static data access in a transaction type.
// Per the Polyjuice paper (§4.2), each access has:
//   - read_version:      CLEAN_READ (0) or DIRTY_READ (1)
//   - write_visibility:  PRIVATE (0) or PUBLIC (1)
//   - early_validation:  whether to validate after this access (piece boundary)
//   - wait_*:            per-txn-type access-id targets (0 = NO_WAIT)
//     The wait action is invoked before the access. If early_validation is set
//     on the previous access, the next access's wait values are reused for the
//     pre-validation wait (§4.2 "we consolidate the two kinds of wait
//     actions").
struct PolicyEntry {
  int read_version;      // 0=CLEAN_READ, 1=DIRTY_READ
  int write_visibility;  // 0=PRIVATE, 1=PUBLIC
  int early_validation;  // piece boundary: validate reads/writes since last
                         // validation
  int wait_new_order;    // wait target access-id for new_order txns (0=NO_WAIT)
  int wait_payment;      // wait target access-id for payment txns (0=NO_WAIT)
  int wait_delivery;     // wait target access-id for delivery txns (0=NO_WAIT)
};

// Entries referenced by access mappings are annotated with their txn/op.
// Other entries participate in the wait chain (wait targets reference
// access-ids which index into this table).
//                       rd  vis  ev  wNO wPAY wDEL
static const PolicyEntry POLICY[26] = {
    {1, 0, 1, 5, 1, 8},   //  0: new_order op 0 — warehouse RD
    {1, 1, 0, 1, 4, 8},   //  1
    {1, 1, 1, 2, 1, 0},   //  2: new_order op 2 — district WR
    {0, 0, 1, 2, 3, 4},   //  3: new_order op 3 — item RD
    {1, 0, 1, 2, 0, 2},   //  4
    {0, 1, 1, 2, 1, 1},   //  5: new_order op 4 — stock WR
    {1, 1, 0, 7, 6, 7},   //  6
    {0, 0, 1, 10, 0, 2},  //  7
    {0, 1, 0, 2, 4, 6},   //  8
    {0, 0, 0, 9, 0, 5},   //  9
    {0, 0, 0, 5, 2, 8},   // 10: new_order op 1 — customer RD
    {1, 0, 0, 0, 5, 0},   // 11: payment op 0   — warehouse WR
    {0, 1, 1, 5, 7, 7},   // 12
    {1, 1, 0, 1, 2, 8},   // 13
    {1, 0, 1, 0, 3, 5},   // 14: payment op 1   — district WR
    {0, 1, 0, 3, 4, 8},   // 15
    {1, 1, 1, 0, 1, 0},   // 16: payment op 2   — customer WR
    {1, 1, 0, 7, 6, 2},   // 17
    {1, 0, 0, 5, 0, 5},   // 18
    {0, 0, 1, 4, 0, 8},   // 19
    {0, 1, 1, 7, 7, 1},   // 20
    {0, 0, 1, 7, 6, 2},   // 21
    {0, 0, 1, 8, 7, 4},   // 22
    {1, 0, 0, 10, 1, 0},  // 23
    {0, 1, 0, 8, 5, 1},   // 24
    {0, 1, 0, 1, 7, 6},   // 25
};

enum TpccTxnType { TXN_NEW_ORDER = 0, TXN_PAYMENT = 1 };

// ---- Access mapping ----
// Maps runtime op_index (from tpcc_txn_pj.cpp's op_cnt) to a PolicyEntry and
// step value. Where Polyjuice has separate RD and WR access-ids for the same
// row, we consolidate into one get_row and map to the WR's policy entry.
struct OpMapping {
  int policy_index;  // Polyjuice acc_id (index into POLICY[])
  int step;          // progress value published for wait tracking
};

// new_order layout (from tpcc_txn_pj.cpp):
//   prefix ops:                fixed count, before loops
//   item loop:                 ol_cnt ops, all use ITEM entry
//   stock loop:                ol_cnt ops, all use STOCK entry
//   suffix op (customer RD):   1 op, after loops
static constexpr std::array<OpMapping, 2> NO_PREFIX = {{
    {0, 1},  // op 0: warehouse RD  (acc_id 0)
    {2, 3},  // op 1: district WR   (acc_ids 1+2 consolidated)
}};
static constexpr OpMapping NO_ITEM = {3, 4};        // acc_id 3
static constexpr OpMapping NO_STOCK = {5, 6};       // acc_ids 4+5 consolidated
static constexpr OpMapping NO_CUSTOMER = {10, 11};  // acc_id 10
// Max step values per txn type (published at final commit to unblock waiters).
static constexpr int NO_MAX_STEP = 11;  // new_order: customer RD is last
static constexpr int PAY_MAX_STEP = 7;  // payment: acc_id 17 (history insert)

// payment layout: 3 fixed ops, no loops. Order matches the non-commutative
// build of Polyjuice (tpcc.cc:2705, SUPPORT_COMMUTATIVE_OP undefined), which
// is what the 48th-4wh.txt policy was trained against.
static constexpr std::array<OpMapping, 3> PAY_OPS = {{
    {12, 2},  // op 0: warehouse (acc_ids 11+12 consolidated → warehouse WR)
    {14, 4},  // op 1: district  (acc_ids 13+14 consolidated → district WR)
    {16, 6},  // op 2: customer  (acc_ids 15+16 consolidated → customer WR)
}};

static inline const PolicyEntry* lookup_policy(TpccTxnType txn_type,
                                               int op_index, int ol_cnt,
                                               int* step) {
  const OpMapping* m;
  if (txn_type == TXN_NEW_ORDER) {
    // new_order: prefix, item loop, stock loop, customer suffix
    if (op_index < (int)NO_PREFIX.size()) {
      m = &NO_PREFIX[op_index];
    } else if (op_index < (int)NO_PREFIX.size() + ol_cnt) {
      m = &NO_ITEM;
    } else if (op_index < (int)NO_PREFIX.size() + 2 * ol_cnt) {
      m = &NO_STOCK;
    } else if (op_index == (int)NO_PREFIX.size() + 2 * ol_cnt) {
      m = &NO_CUSTOMER;
    } else {
      printf("ERROR: new_order op_index %d out of range (ol_cnt=%d)\n",
             op_index, ol_cnt);
      exit(1);
    }
  } else {
    if (op_index < (int)PAY_OPS.size()) {
      m = &PAY_OPS[op_index];
    } else {
      printf("ERROR: payment op_index %d out of range\n", op_index);
      exit(1);
    }
  }
  *step = m->step;
  return &POLICY[m->policy_index];
}

// ---- Per-row state ----
static constexpr uint64_t LOCK_BIT = 1ULL << 63;

// Linked list of uncommitted write entries per row. Multiple transactions
// can have exposed (but uncommitted) writes on the same row. The head of
// the list is the latest writer. This replicates Polyjuice's per-tuple
// access list, enabling write-write dependency tracking: a later writer
// records a dependency on all prior uncommitted writers, ensuring commit
// ordering without cascading aborts.
struct DirtyEntry {
  txn_man* writer;
  uint64_t txn_seq;  // writer's TxnManState::txn_seq when entry was created
  char* data;
  DirtyEntry* next;
};

// Concurrency invariants for RowState:
//   tid_word: read via volatile load; written atomically (CAS for lock,
//     atomic AND for unlock, direct store only while lock is held).
//   dirty_head: read/written only while LOCK_BIT is held. Readers in
//     cc_post_op acquire spin_lock before traversing; writers in
//     piece_validate_and_expose hold WriteLockSet (which holds LOCK_BIT).
//     A COMPILER_BARRIER after linking a new entry ensures fields are
//     visible before the entry becomes reachable via dirty_head.
struct RowState {
  volatile uint64_t tid_word;  // version TID | LOCK_BIT
  DirtyEntry* dirty_head;  // linked list of uncommitted writes (latest first)
};

static inline uint64_t get_tid(RowState* s) { return s->tid_word & ~LOCK_BIT; }
static inline bool try_lock(RowState* s) {
  uint64_t v = s->tid_word;
  if (v & LOCK_BIT) {
    return false;
  }
  return __sync_bool_compare_and_swap(&s->tid_word, v, v | LOCK_BIT);
}
static inline void spin_lock(RowState* s) {
  while (true) {
    uint64_t v = s->tid_word;
    if (!(v & LOCK_BIT) &&
        __sync_bool_compare_and_swap(&s->tid_word, v, v | LOCK_BIT)) {
      return;
    }
    PAUSE
  }
}
static inline void unlock(RowState* s) {
  // Must be atomic: a plain RMW (&=) could lose concurrent tid_word
  // updates from other threads (e.g., another txn's final_commit writing
  // a new commit_tid while we clear LOCK_BIT).
  __sync_fetch_and_and(&s->tid_word, ~LOCK_BIT);
}

// Silo-style OCC snapshot: copy orig_row data into dest while ensuring the
// row isn't being modified. Spins until tid_word is stable across the copy.
// Returns the observed tid (without LOCK_BIT).
// Thread-safety: reads tid_word (volatile) and row data without locks.
// If a concurrent writer holds LOCK_BIT, we spin. If a writer commits
// between our two tid_word reads, we detect the change and retry.
static inline uint64_t occ_snapshot(row_t* orig, row_t* dest) {
  RowState* rs = (RowState*)orig->cc_row_state;
  uint64_t v, v2;
  do {
    v = rs->tid_word;
    while (v & LOCK_BIT) {
      PAUSE
      v = rs->tid_word;
    }
    dest->copy(orig);
    COMPILER_BARRIER
    v2 = rs->tid_word;
  } while (v != v2);
  return v & ~LOCK_BIT;
}

// ---- Per-txn state ----
// TxnManState is allocated once per txn_man (on first use) and reused across
// transactions. Per-txn fields are reset in cc_pre_txn; persistent fields
// (txn_seq, history ring buffer) survive across transactions for dependency
// resolution.
//
// Concurrency invariants:
//   - Per-txn fields (ol_cnt, deps, reads, writes, piece_*,
//     pending_piece_validation): written only by the owning thread, not
//     read by other threads. No synchronization needed.
//   - txn_type: set by owner in cc_pre_txn, constant for the txn's
//     lifetime. Read by other threads when creating dependencies
//     (e.g., get_tms(de->writer)->txn_type in cc_post_op). Safe because
//     the value doesn't change while the txn is running.
//   - step: written by owner (cc_post_op, final_commit), read by other
//     threads in do_wait (dep->writer's step >= target). Declared
//     volatile. A stale read means the waiter spins one extra iteration
//     — the next read will see the updated value. Correct on x86 (TSO);
//     on weaker architectures, use std::atomic with release/acquire.
//   - txn_seq: written by owner once at cc_pre_txn start, then constant
//     for the txn's lifetime. Read by other threads in check_dep_status.
//     A stale read (seeing old txn_seq) makes the reader think the dep
//     txn is still running, which is safe — the reader's spin loop
//     retries, and will see the updated txn_seq on the next iteration.
//     The result may already be in the ring buffer for the next check.
//   - history[]: written by owner (publish_txn_result) using seqlock
//     protocol (txn_seq as version). Read by other threads in
//     lookup_txn_result with double-read validation.
//   - last_commit_tid: written and read only by the owning thread.
//     Copied into history[].commit_tid by publish_txn_result (value
//     copy, not pointer — other threads read the copy, not this field).
enum TxnStatus {
  TXN_RUNNING = 0,
  TXN_COMMITTED = 1,
  TXN_ABORTED = 2,
  // The dependent txn has finished but its outcome was evicted from the
  // ring buffer before we could read it (the writer's txn_man completed
  // more than HISTORY_SIZE transactions since the dependency was created).
  // Treated conservatively: cascade-abort for dirty-read deps.
  TXN_UNKNOWN = 3,
};
struct Dependency {
  txn_man* writer;
  uint64_t txn_seq;  // writer's TxnManState::txn_seq when dep was created
  TpccTxnType dep_txn_type;  // writer's txn type, captured at creation time
  bool from_dirty_read;
};
struct ReadEntry {
  row_t* row;
  uint64_t tid;
  bool dirty;              // true if this read was a dirty read
  txn_man* dirty_writer;   // writer of the dirty entry (only if dirty)
  uint64_t dirty_txn_seq;  // writer's txn_seq (only if dirty)
};
struct WriteEntry {
  row_t* orig_row;
  row_t* local_copy;
  bool to_expose;  // true if write_visibility == PUBLIC (expose at validation)
  bool exposed;    // true if dirty data has been exposed via early-validation
};
static constexpr int MAX_ACCESSES = 64;
static constexpr int MAX_DEPS = 64;
struct TxnManState {
  // -- Per-txn fields (reset each transaction) --
  TpccTxnType txn_type;
  int ol_cnt;         // order-line count (new_order only)
  volatile int step;  // current progress for wait tracking
  // Step for the most recent access, published to `step` only at the NEXT
  // hook call (cc_pre_op or cc_pre_commit). Deferred because in DBx1000 a
  // write happens as a plain memory store by txn logic AFTER cc_post_op
  // returns — at cc_post_op time the write buffer still holds pre-write
  // data, so a waiter unblocked by an early step bump would observe stale
  // values. Deferring ensures that by the time `step` advances, the txn
  // has either finished the previous op's write OR is about to call the
  // next CC hook (so downstream effects are finalized in memory).
  int pending_step;
  Dependency deps[MAX_DEPS];
  int dep_count;
  ReadEntry reads[MAX_ACCESSES];
  int read_count;
  WriteEntry writes[MAX_ACCESSES];
  int write_count;
  int piece_read_start;           // read_count at start of current piece
  int piece_write_start;          // write_count at start of current piece
  bool pending_piece_validation;  // early-validation pending before next op

  // -- Persistent fields (survive across transactions) --
  // Per Silo §4.2, a worker's TID must be larger than its most recently
  // chosen TID (condition b), ensuring same-worker txns are ordered.
  uint64_t last_commit_tid = 0;
  // Per-txn_man sequence number, incremented each cc_pre_txn. NOT a commit
  // timestamp (that's computed in final_commit as a Silo-style TID). This is
  // purely a generation counter for dependency tracking: when a waiter holds
  // a Dependency with txn_seq=N, it compares against the writer's current
  // txn_seq to detect whether the writer has moved on to a new transaction.
  // Starts at 0, bumped to 1 before the first txn, so valid values are >= 1.
  // This makes 0 a safe sentinel for uninitialized ring buffer slots.
  volatile uint64_t txn_seq = 0;
  // Ring buffer of recent completed txn outcomes (seqlock protocol).
  // Slot validity: txn_seq == 0 means uninitialized (never written),
  // txn_seq == HISTORY_UPDATING means write in progress, otherwise valid.
  // Readers skip slots where txn_seq doesn't match the target.
  struct TxnResult {
    volatile uint64_t txn_seq;  // 0=uninitialized, UPDATING=write in progress
    volatile TxnStatus status;
    volatile uint64_t commit_tid;  // Silo TID assigned at commit (for dirty
                                   // read validation by other txns)
  };
  static constexpr int HISTORY_SIZE = 8;
  static constexpr uint64_t HISTORY_UPDATING = UINT64_MAX;
  std::array<TxnResult, HISTORY_SIZE> history = {};
  int history_head = 0;
};

// Helper to get the TxnManState from a txn_man (may be null for non-PJ txns).
static inline TxnManState* get_tms(txn_man* tx) {
  return (TxnManState*)tx->cc_txn_state;
}

// ---- Dependency resolution helpers ----

// Publish a completed txn's outcome to the ring buffer (seqlock protocol).
static inline void publish_txn_result(TxnManState* tms, TxnStatus status,
                                      uint64_t commit_tid) {
  int slot = tms->history_head;
  tms->history[slot].txn_seq = TxnManState::HISTORY_UPDATING;
  COMPILER_BARRIER
  tms->history[slot].status = status;
  tms->history[slot].commit_tid = commit_tid;
  COMPILER_BARRIER
  tms->history[slot].txn_seq = tms->txn_seq;
  tms->history_head = (slot + 1) % TxnManState::HISTORY_SIZE;
}

// Look up a txn's outcome in the writer's ring buffer.
struct TxnResultSnapshot {
  TxnStatus status;
  uint64_t commit_tid;
};

static inline bool lookup_txn_result(TxnManState* writer_tms, uint64_t txn_seq,
                                     TxnResultSnapshot* out) {
  for (int i = 0; i < TxnManState::HISTORY_SIZE; i++) {
    uint64_t id1;

    // If write in progress, spin until it completes, then check.
    while ((id1 = writer_tms->history[i].txn_seq) ==
           TxnManState::HISTORY_UPDATING) {
      PAUSE
    }
    if (id1 == 0 || id1 != txn_seq) {  // 0 = unused
      continue;
    }

    COMPILER_BARRIER
    TxnStatus s = writer_tms->history[i].status;
    uint64_t ct = writer_tms->history[i].commit_tid;
    COMPILER_BARRIER
    uint64_t id2 = writer_tms->history[i].txn_seq;
    if (id1 == id2) {
      out->status = s;
      out->commit_tid = ct;
      return true;
    }
  }
  return false;
}

// Add a dependency, deduplicating by (writer, txn_seq). If an existing dep
// with from_dirty_read=false is found and the new one is true, upgrade it.
// Returns false if the dep array is full (caller should abort).
static inline bool add_dependency(TxnManState* tms, txn_man* writer,
                                  uint64_t txn_seq, TpccTxnType dep_txn_type,
                                  bool from_dirty_read) {
  for (int i = 0; i < tms->dep_count; i++) {
    if (tms->deps[i].writer == writer && tms->deps[i].txn_seq == txn_seq) {
      if (from_dirty_read) {
        tms->deps[i].from_dirty_read = true;
      }
      return true;
    }
  }
  if (tms->dep_count >= MAX_DEPS) {
    return false;
  }
  tms->deps[tms->dep_count] = {writer, txn_seq, dep_txn_type, from_dirty_read};
  tms->dep_count++;
  return true;
}

static inline TxnStatus check_dep_status(Dependency* dep,
                                         uint64_t* commit_tid_out = nullptr) {
  TxnManState* ws = get_tms(dep->writer);
  assert(ws);

  // Stale txn_seq read is safe: if we see the old value, we return
  // TXN_RUNNING. The caller (do_wait) spins and retries. On the next
  // call, we'll see the updated txn_seq and check the ring buffer.
  if (ws->txn_seq == dep->txn_seq) {
    return TXN_RUNNING;
  }

  // Ring buffer is the authoritative source for completed txn outcomes.
  TxnResultSnapshot snap;
  if (lookup_txn_result(ws, dep->txn_seq, &snap)) {
    if (commit_tid_out) {
      *commit_tid_out = snap.commit_tid;
    }
    return snap.status;
  }

  // Not in ring buffer: either still running, or evicted.
  return TXN_UNKNOWN;
}

// ---- Wait ----
static const uint64_t WAIT_TIMEOUT = 100000;
// Wait on dependencies. If policy is provided, wait until each dep reaches
// its policy-specified step target. If policy is null (pre-commit), wait
// until each dep finishes (commit/abort).
static RC do_wait(TxnManState* tms, const PolicyEntry* policy = nullptr) {
  for (int d = 0; d < tms->dep_count; d++) {
    Dependency* dep = &tms->deps[d];

    // Determine wait target from policy, or 0 to wait until finished.
    int target = 0;
    if (policy) {
      target = (dep->dep_txn_type == TXN_NEW_ORDER) ? policy->wait_new_order
                                                    : policy->wait_payment;
      if (!target) {
        continue;
      }
    }

    // Pre-commit waits (policy=null) use a longer timeout since we're
    // waiting for the dep to fully commit, not just reach a step.
    uint64_t timeout = policy ? WAIT_TIMEOUT : WAIT_TIMEOUT * 10;
    uint64_t t0 = get_sys_clock();
    while (true) {
      TxnStatus s = check_dep_status(dep);
      if (s != TXN_RUNNING) {
        if ((s == TXN_ABORTED || s == TXN_UNKNOWN) && dep->from_dirty_read) {
          return Abort;
        }
        break;
      }
      // Stale step read is safe: we just spin one more iteration.
      if (target && get_tms(dep->writer)->step >= target) {
        break;
      }
      if (get_sys_clock() - t0 > timeout) {
        return Abort;
      }
      PAUSE
    }
  }
  return RCOK;
}

// ---- Write-set locking helper ----
// Locks write-set rows in address order to prevent deadlocks. On failure
// (try_lock fails), unlocks all already-held locks and returns false.
struct WriteLockSet {
  int indices[MAX_ACCESSES];  // write indices sorted by orig_row address
  int count = 0;

  // Sort writes[begin..end) by orig_row address and try to lock all.
  bool lock_sorted(TxnManState* tms, int begin, int end) {
    count = end - begin;
    for (int i = 0; i < count; i++) {
      indices[i] = begin + i;
    }
    std::sort(indices, indices + count, [&](int a, int b) {
      return (uintptr_t)tms->writes[a].orig_row <
             (uintptr_t)tms->writes[b].orig_row;
    });
    for (int i = 0; i < count; i++) {
      RowState* rs = (RowState*)tms->writes[indices[i]].orig_row->cc_row_state;
      if (!try_lock(rs)) {
        for (int j = 0; j < i; j++) {
          unlock((RowState*)tms->writes[indices[j]].orig_row->cc_row_state);
        }
        return false;
      }
    }
    return true;
  }

  void unlock_all(TxnManState* tms) {
    for (int i = 0; i < count; i++) {
      unlock((RowState*)tms->writes[indices[i]].orig_row->cc_row_state);
    }
  }
};

// Validate reads[r_begin..r_end) against current tid_word. Rows locked
// by writes[w_begin..w_end) are recognized as our own (LOCK_BIT OK).
// all_deps_resolved: if true (final commit), assert no dirty reads have
// running deps — caller must have waited for all deps first.
static bool validate_reads(TxnManState* tms, int r_begin, int r_end,
                           int w_begin, int w_end,
                           bool all_deps_resolved = false) {
  for (int i = r_begin; i < r_end; i++) {
    if (tms->reads[i].dirty) {
      // Dirty read validation (Polyjuice §4.2 early-validation).
      Dependency dep = {
          tms->reads[i].dirty_writer, tms->reads[i].dirty_txn_seq, {}, true};
      uint64_t dep_commit_tid = 0;
      TxnStatus s = check_dep_status(&dep, &dep_commit_tid);
      if (s == TXN_ABORTED || s == TXN_UNKNOWN) {
        return false;
      }
      if (s == TXN_COMMITTED) {
        // Dep committed: convert to clean read with dep's commit_tid
        // and fall through to normal tid validation below.
        tms->reads[i].dirty = false;
        tms->reads[i].tid = dep_commit_tid;
      } else {
        // Dep still running: OK during early validation (piece boundary),
        // but must not happen at final commit (caller should have waited).
        assert(!all_deps_resolved);
        continue;
      }
    }
    RowState* rs = (RowState*)tms->reads[i].row->cc_row_state;
    uint64_t v = rs->tid_word;
    if (v & LOCK_BIT) {
      bool own_write = false;
      for (int j = w_begin; j < w_end; j++) {
        if (tms->writes[j].orig_row == tms->reads[i].row) {
          own_write = true;
          break;
        }
      }
      if (!own_write) {
        return false;
      }
      v &= ~LOCK_BIT;
    }
    if (tms->reads[i].tid != v) {
      return false;
    }
  }
  return true;
}

// ---- Piece validate+expose (NOT install to orig_row) ----
// Validates reads/writes in the current piece and, for PUBLIC writes,
// exposes dirty data so other txns can dirty-read it.
// Per §4.2: "we defer appending reads and visible-writes to their
// corresponding access lists until a successful early-validation."
static RC piece_validate_and_expose(txn_man* txn) {
  TxnManState* tms = get_tms(txn);
  int pr = tms->piece_read_start;
  int pre = tms->read_count;
  int pw = tms->piece_write_start;
  int pwe = tms->write_count;
  // Lock write rows in address order to prevent deadlocks.
  WriteLockSet wlocks;
  if (!wlocks.lock_sorted(tms, pw, pwe)) {
    return Abort;
  }

  if (!validate_reads(tms, pr, pre, pw, pwe)) {
    wlocks.unlock_all(tms);
    return Abort;
  }
  // Expose PUBLIC writes to dirty list; PRIVATE writes stay local.
  // Record write-write deps on prior uncommitted writers regardless.
  for (int i = pw; i < pwe; i++) {
    WriteEntry& w = tms->writes[i];
    RowState* rs = (RowState*)w.orig_row->cc_row_state;
    // Add write-write dependencies on prior uncommitted writers
    for (DirtyEntry* e = rs->dirty_head; e; e = e->next) {
      if (e->writer != txn &&
          !add_dependency(tms, e->writer, e->txn_seq,
                          get_tms(e->writer)->txn_type, false)) {
        wlocks.unlock_all(tms);
        return Abort;
      }
    }

    // Expose dirty data so other txns can dirty-read it.
    if (w.to_expose) {
      uint32_t sz = w.local_copy->get_tuple_size();
      DirtyEntry* entry = (DirtyEntry*)malloc(sizeof(DirtyEntry));
      entry->writer = txn;
      entry->txn_seq = tms->txn_seq;
      entry->data = (char*)malloc(sz);
      memcpy(entry->data, w.local_copy->get_data(), sz);
      entry->next = rs->dirty_head;
      rs->dirty_head = entry;
      w.exposed = true;
    }
  }
  wlocks.unlock_all(tms);
  tms->piece_read_start = tms->read_count;
  tms->piece_write_start = tms->write_count;
  return RCOK;
}

// ---- Final commit: validate ALL + copy local→orig atomically ----
static RC final_commit(txn_man* txn) {
  TxnManState* tms = get_tms(txn);
  // Lock write rows in address order to prevent deadlocks.
  WriteLockSet wlocks;
  if (!wlocks.lock_sorted(tms, 0, tms->write_count)) {
    return Abort;
  }
  // NOTE: validate_reads may convert dirty reads to clean reads in-place
  // (setting reads[i].dirty=false, reads[i].tid=dep_commit_tid) when the
  // dep has committed. This is intentional — the converted entries then
  // participate in the normal tid validation below.
  if (!validate_reads(tms, 0, tms->read_count, 0, tms->write_count,
                      /*all_deps_resolved=*/true)) {
    wlocks.unlock_all(tms);
    return Abort;
  }
  // Compute commit TID per Silo §4.2: must be greater than (a) all
  // read/written row tids and (b) this worker's last chosen TID.
  // We skip condition (c) (epoch) as we don't implement Silo's epoch GC.
  uint64_t max_tid = tms->last_commit_tid;
  for (int i = 0; i < tms->read_count; i++) {
    if (tms->reads[i].tid > max_tid) {
      max_tid = tms->reads[i].tid;
    }
  }
  for (int i = 0; i < tms->write_count; i++) {
    uint64_t t = get_tid((RowState*)tms->writes[i].orig_row->cc_row_state);
    if (t > max_tid) {
      max_tid = t;
    }
  }
  uint64_t commit_tid = max_tid + 1;
  tms->last_commit_tid = commit_tid;
  // Install: local_copy → orig_row, remove our entry from dirty list
  for (int i = 0; i < tms->write_count; i++) {
    WriteEntry& w = tms->writes[i];
    RowState* rs = (RowState*)w.orig_row->cc_row_state;
    uint32_t sz = w.orig_row->get_tuple_size();
    memcpy(w.orig_row->get_data(), w.local_copy->get_data(), sz);
    rs->tid_word = commit_tid | LOCK_BIT;
    // Remove our dirty_head entry (only exists for PUBLIC writes).
    if (!w.exposed) {
      continue;
    }
    DirtyEntry** pp = &rs->dirty_head;
    while (*pp) {
      if ((*pp)->writer == txn) {
        DirtyEntry* victim = *pp;
        *pp = victim->next;
        free(victim->data);
        free(victim);
        break;
      }
      pp = &(*pp)->next;
    }
  }
  wlocks.unlock_all(tms);
  tms->step = (tms->txn_type == TXN_NEW_ORDER) ? NO_MAX_STEP : PAY_MAX_STEP;
  return RCOK;
}

// ---- Hook implementations ----

void cc_init_row_state(row_t* r) {
  RowState* s = (RowState*)malloc(sizeof(RowState));
  s->tid_word = 0;
  s->dirty_head = nullptr;
  r->cc_row_state = s;
}
void cc_free_row_state(row_t* r) {
  RowState* s = (RowState*)r->cc_row_state;
  if (s) {
    DirtyEntry* e = s->dirty_head;
    while (e) {
      DirtyEntry* next = e->next;
      free(e->data);
      free(e);
      e = next;
    }
    free(s);
    r->cc_row_state = nullptr;
  }
}
void cc_global_init() {}

void cc_init_txn_man(txn_man* tx) {
  auto* tms = (TxnManState*)_mm_malloc(sizeof(TxnManState), 64);
  new (tms) TxnManState{};
  tx->cc_txn_state = tms;
}

// ---- Learned adaptive backoff (Polyjuice §4.3) ----
// Per-thread backoff state, adjusted using learned multipliers from the policy.
// On abort: backoff *= (1 + mult * ALPHA), then spin for `backoff` nop_pauses.
// On commit: backoff /= (1 + mult * ALPHA), no spin.
// `mult` is a per-(success/failure, retry_count, txn_type) learned value.
// ALPHA is a global scaling constant (from Polyjuice bench.cc:51).

// Learned multipliers from 48th-4wh.txt policy file (TPCC with 3 txn types:
// NEW_ORDER, PAYMENT, DELIVERY — one value per column, in that order).
// We use the NO and PAY columns; DELIVERY is unused in DBx1000 TPCC.
// Dimensions: [2][3][2] = [success(1)/failure(0)][retry 0,1,>=2][NO,PAY]
static constexpr double BACKOFF_MULT[2][3][2] = {
    // [0] = failure (increase backoff)    file lines: "0 8 1" "2 8 1" "2 0 0"
    {{0, 8}, {2, 8}, {2, 0}},
    // [1] = success (decrease backoff)    file lines: "0 1 0" "0 1 4" "1 4 1"
    {{0, 1}, {0, 1}, {1, 4}},
};
static constexpr double BACKOFF_ALPHA = 0.5;            // bench.cc:51
static constexpr uint64_t BACKOFF_FLOOR = 100;          // bench.h:253
static constexpr uint64_t BACKOFF_CAP = 6710886400ULL;  // bench.h:248

struct BackoffState {
  uint64_t backoff = BACKOFF_FLOOR;
  int retry_count = 0;
};
static thread_local BackoffState pj_backoff[2];  // [TXN_NEW_ORDER, TXN_PAYMENT]

static inline void adjust_backoff(BackoffState& bs, TpccTxnType type,
                                  bool success) {
  int retry = bs.retry_count > 2 ? 2 : bs.retry_count;
  double mult = BACKOFF_MULT[success ? 1 : 0][retry][type];
  double factor = 1.0 + mult * BACKOFF_ALPHA;
  if (success) {
    bs.backoff = static_cast<uint64_t>(bs.backoff / factor);
    if (bs.backoff < BACKOFF_FLOOR) {
      bs.backoff = BACKOFF_FLOOR;
    }
    bs.retry_count = 0;
  } else {
    bs.backoff = static_cast<uint64_t>(bs.backoff * factor);
    if (bs.backoff > BACKOFF_CAP) {
      bs.backoff = BACKOFF_CAP;
    }
    bs.retry_count++;
  }
}

void cc_pre_txn(thread_t* th, txn_man* tx, base_query* q) {
  (void)th;
  tpcc_query* tq = (tpcc_query*)q;
  TpccTxnType txn_type =
      (tq->type == TPCC_NEW_ORDER) ? TXN_NEW_ORDER : TXN_PAYMENT;
  // Backoff: spin for learned number of nop_pauses before retrying.
  BackoffState& bs = pj_backoff[txn_type];
  if (bs.retry_count > 0) {
    uint64_t spins = bs.backoff;
    while (spins--) {
      PAUSE
    }
  }
  // TxnManState allocated in cc_init_txn_man, reused across transactions.
  TxnManState* tms = get_tms(tx);
  assert(tms);
  // Increment persistent txn_seq; reset per-txn fields.
  tms->txn_seq = tms->txn_seq + 1;
  tms->txn_type = txn_type;
  tms->ol_cnt = (txn_type == TXN_NEW_ORDER) ? tq->ol_cnt : 0;
  tms->step = 0;
  tms->pending_step = 0;

  tms->dep_count = 0;
  tms->read_count = 0;
  tms->write_count = 0;
  tms->piece_read_start = 0;
  tms->piece_write_start = 0;
  tms->pending_piece_validation = false;
}

void cc_post_txn(thread_t* th, txn_man* tx, RC r) {
  (void)th;
  TxnManState* tms = get_tms(tx);
  assert(tms);
  adjust_backoff(pj_backoff[tms->txn_type], tms->txn_type, r == RCOK);
  // Publish outcome to ring buffer. TxnManState is NOT freed — reused next txn.
  TxnStatus final_status = (r == RCOK) ? TXN_COMMITTED : TXN_ABORTED;
  publish_txn_result(tms, final_status, tms->last_commit_tid);

  // On abort, remove any exposed dirty entries from rows' dirty_head lists.
  // (On commit, final_commit already removed them when installing data.)
  if (r != RCOK) {
    for (int i = 0; i < tms->write_count; i++) {
      if (!tms->writes[i].exposed) {
        continue;
      }
      row_t* orig = tms->writes[i].orig_row;
      RowState* rs = (RowState*)orig->cc_row_state;
      spin_lock(rs);
      DirtyEntry** pp = &rs->dirty_head;
      while (*pp) {
        if ((*pp)->writer == tx) {
          DirtyEntry* victim = *pp;
          *pp = victim->next;
          free(victim->data);
          free(victim);
          break;
        }
        pp = &(*pp)->next;
      }
      unlock(rs);
    }
  }

  // Local copies (reads and writes) are freed in cc_release_op, which
  // receives each access's local pointer and can free them uniformly.
}

RC cc_pre_op(txn_man* txn, row_t* orig, access_t type, int op) {
  (void)orig;
  (void)type;
  TxnManState* tms = (TxnManState*)txn->cc_txn_state;
  int step;
  const PolicyEntry* policy =
      lookup_policy(tms->txn_type, op, tms->ol_cnt, &step);
  (void)step;  // step for this op published at the NEXT hook call

  // Per the paper (§4.2): "we consolidate the two kinds of wait actions
  // into one. Polyjuice uses the wait action corresponding to the next
  // access-id if early-validation is enabled for the current access-id."
  // This wait is simultaneously (a) the pre-access wait for THIS op and
  // (b) the pre-validation wait for the PREVIOUS piece — so it must run
  // before piece_validate_and_expose.
  if (do_wait(tms, policy) != RCOK) {
    return Abort;
  }

  // Finalize the PREVIOUS op: run any pending piece-validation (which also
  // links exposed dirty writes into dirty_head), then publish pending_step
  // so waiters observe our latest progress. Order matters: expose-then-
  // advance. If we advanced step first, waiters doing dirty-reads could
  // unblock, inspect dirty_head before expose completes, find nothing,
  // and fall through to a clean read of stale (pre-write) data.
  if (tms->pending_piece_validation) {
    tms->pending_piece_validation = false;
    if (piece_validate_and_expose(txn) != RCOK) {
      return Abort;
    }
  }
  if (tms->pending_step > tms->step) {
    tms->step = tms->pending_step;
  }
  return RCOK;
}

// NOTE: read-your-own-writes via dirty_head is NOT supported. If a txn
// writes a row and later reads it, it must keep the value in a local
// variable — do not re-read from the database. (tpcc_txn_pj.cpp does
// not do this, so it's safe for our current workloads.)
RC cc_post_op(txn_man* txn, row_t* orig, row_t** local_row_out, access_t type,
              int op) {
  TxnManState* tms = (TxnManState*)txn->cc_txn_state;
  RowState* rs = (RowState*)orig->cc_row_state;
  int step;
  const PolicyEntry* policy =
      lookup_policy(tms->txn_type, op, tms->ol_cnt, &step);

  // Capacity check before allocating anything.
  if (tms->read_count >= MAX_ACCESSES) {
    return Abort;
  }
  if (type == WR && tms->write_count >= MAX_ACCESSES) {
    return Abort;
  }

  // All paths (RD, WR, dirty, clean) allocate a local copy.
  row_t* copy = (row_t*)_mm_malloc(sizeof(row_t), 64);
  copy->init(orig->get_table(), orig->get_part_id());

  // Try dirty read if policy says so (RD/SCAN only).
  bool dirty = false;
  txn_man* dirty_writer = nullptr;
  uint64_t dirty_txn_seq = 0;
  if ((type == RD || type == SCAN) && policy->read_version == 1) {
    spin_lock(rs);
    DirtyEntry* de = rs->dirty_head;
    if (de) {
      if (!add_dependency(tms, de->writer, de->txn_seq,
                          get_tms(de->writer)->txn_type, true)) {
        unlock(rs);
        copy->free_row();
        _mm_free(copy);
        return Abort;
      }
      memcpy(copy->get_data(), de->data, orig->get_tuple_size());
      dirty = true;
      dirty_writer = de->writer;
      dirty_txn_seq = de->txn_seq;
    }
    unlock(rs);
  }

  // If not dirty, OCC snapshot from committed data.
  uint64_t tid = 0;
  if (!dirty) {
    tid = occ_snapshot(orig, copy);
  }

  *local_row_out = copy;

  // Record read entry (all ops implicitly read).
  tms->reads[tms->read_count] = {orig, tid, dirty, dirty_writer, dirty_txn_seq};
  tms->read_count++;

  // WR: also record write entry.
  if (type == WR) {
    bool to_expose = (policy->write_visibility == 1);
    tms->writes[tms->write_count] = {orig, copy, to_expose, false};
    tms->write_count++;
  }

  // Defer step publication until the next hook call — at post_op time a
  // WR op's local copy has not yet been modified by the txn logic, so a
  // waiter unblocked here would read stale memory. See `pending_step`
  // doc on TxnManState.
  if (step > tms->pending_step) {
    tms->pending_step = step;
  }
  if (policy->early_validation) {
    tms->pending_piece_validation = true;
  }
  return RCOK;
}

RC cc_pre_commit(txn_man* txn) {
  TxnManState* tms = get_tms(txn);
  // Finalize the last op: flush pending piece validation, then publish the
  // pending step so waiters observe our latest progress before we block.
  if (tms->pending_piece_validation) {
    tms->pending_piece_validation = false;
    if (piece_validate_and_expose(txn) != RCOK) {
      return Abort;
    }
  }
  if (tms->pending_step > tms->step) {
    tms->step = tms->pending_step;
  }
  // Wait for all dependencies to finish (no step target = wait until done).
  if (do_wait(tms) != RCOK) {
    return Abort;
  }
  return final_commit(txn);
}

void cc_release_op(txn_man* txn, row_t* orig, row_t* local, access_t type,
                   int op) {
  (void)txn;
  (void)op;
  // Free local copy. Dirty entry cleanup is handled in cc_post_txn.
  // type is WR (commit) or XP (abort) for writes, RD/SCAN for reads.
  if (local && local != orig) {
    local->free_row();
    _mm_free(local);
  }
}
