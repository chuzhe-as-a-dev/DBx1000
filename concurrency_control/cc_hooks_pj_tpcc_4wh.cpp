// cc_hooks_pj_tpcc_4wh.cpp — Faithful Polyjuice learned policy.
// Dirty reads, dependency tracking, piece boundaries, atomic final commit.
// orig_row NEVER modified until final commit. Writes go to local copies.

#include <mm_malloc.h>

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

// payment layout: 3 fixed ops, no loops.
static constexpr std::array<OpMapping, 3> PAY_OPS = {{
    {12, 2},  // op 0: warehouse WR  (acc_ids 11+12 consolidated)
    {14, 4},  // op 1: district WR   (acc_ids 13+14 consolidated)
    {16, 6},  // op 2: customer WR   (acc_ids 15+16 consolidated)
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
#define LOCK_BIT (1ULL << 63)

// Linked list of uncommitted write entries per row. Multiple transactions
// can have exposed (but uncommitted) writes on the same row. The head of
// the list is the latest writer. This replicates Polyjuice's per-tuple
// access list, enabling write-write dependency tracking: a later writer
// records a dependency on all prior uncommitted writers, ensuring commit
// ordering without cascading aborts.
struct DirtyEntry {
  txn_man* writer;
  uint64_t txn_id;
  char* data;
  DirtyEntry* next;
};

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
static inline void unlock(RowState* s) { s->tid_word &= ~LOCK_BIT; }

// ---- Per-txn state ----
// TxnManState is allocated once per txn_man (on first use) and reused across
// transactions. Per-txn fields are reset in cc_pre_txn; persistent fields
// (txn_id, history ring buffer) survive across transactions for dependency
// resolution.
enum TxnStatus { TXN_RUNNING = 0, TXN_COMMITTED = 1, TXN_ABORTED = 2 };
struct Dependency {
  txn_man* writer;
  uint64_t txn_id;  // writer's TxnManState::txn_id when dependency was created
  bool from_dirty_read;
};
struct ReadEntry {
  row_t* row;
  uint64_t tid;
  bool dirty;  // true if this read was a dirty read
};
struct WriteEntry {
  row_t* orig_row;
  row_t* local_copy;
  int policy_index;  // index into POLICY[], for write_visibility check
  bool exposed;      // true if dirty data has been exposed via early-validation
};
#define MAX_ACCESSES 64
#define MAX_DEPS 64
struct TxnManState {
  // -- Per-txn fields (reset each transaction) --
  TpccTxnType txn_type;
  int ol_cnt;         // order-line count (new_order only)
  volatile int step;  // current progress for wait tracking
  volatile TxnStatus status;
  uint64_t commit_tid;  // Silo-style TID computed at validation
  Dependency deps[MAX_DEPS];
  int dep_count;
  ReadEntry reads[MAX_ACCESSES];
  int read_count;
  WriteEntry writes[MAX_ACCESSES];
  int write_count;
  int piece_read_start;       // read_count at start of current piece
  int piece_write_start;      // write_count at start of current piece
  bool pending_piece_validation;  // early-validation pending before next op

  // -- Persistent fields (survive across transactions) --
  // Monotonically increasing id, incremented each cc_pre_txn. Used by
  // dependency tracking to detect when a dependent txn_man has moved on.
  volatile uint64_t txn_id = 0;
  // Ring buffer of recent completed txn outcomes (seqlock protocol).
  static constexpr int HISTORY_SIZE = 8;
  static constexpr uint64_t HISTORY_UPDATING = UINT64_MAX;
  struct TxnResult {
    volatile uint64_t txn_id;
    volatile int status;
  };
  TxnResult history[HISTORY_SIZE] = {};
  int history_head = 0;
};

// Helper to get the TxnManState from a txn_man (may be null for non-PJ txns).
static inline TxnManState* get_txn_state(txn_man* tx) {
  return (TxnManState*)tx->cc_txn_state;
}

// ---- Dependency resolution helpers ----

// Publish a completed txn's outcome to the ring buffer (seqlock protocol).
static inline void publish_txn_result(TxnManState* ts, int status) {
  int slot = ts->history_head;
  ts->history[slot].txn_id = TxnManState::HISTORY_UPDATING;
  COMPILER_BARRIER
  ts->history[slot].status = status;
  COMPILER_BARRIER
  ts->history[slot].txn_id = ts->txn_id;
  ts->history_head = (slot + 1) % TxnManState::HISTORY_SIZE;
}

// Look up a txn's outcome in the writer's ring buffer.
static inline bool lookup_txn_result(TxnManState* writer_ts, uint64_t txn_id,
                                     int* status) {
  for (int i = 0; i < TxnManState::HISTORY_SIZE; i++) {
    uint64_t id1 = writer_ts->history[i].txn_id;
    if (id1 == TxnManState::HISTORY_UPDATING || id1 != txn_id) {
      continue;
    }
    COMPILER_BARRIER
    int s = writer_ts->history[i].status;
    COMPILER_BARRIER
    uint64_t id2 = writer_ts->history[i].txn_id;
    if (id1 == id2) {
      *status = s;
      return true;
    }
  }
  return false;
}

// Add a dependency, deduplicating by (writer, txn_id). If an existing dep
// with from_dirty_read=false is found and the new one is true, upgrade it.
static inline void add_dependency(TxnManState* ts, txn_man* writer,
                                  uint64_t txn_id, bool from_dirty_read) {
  for (int i = 0; i < ts->dep_count; i++) {
    if (ts->deps[i].writer == writer && ts->deps[i].txn_id == txn_id) {
      if (from_dirty_read) {
        ts->deps[i].from_dirty_read = true;
      }
      return;
    }
  }
  if (ts->dep_count < MAX_DEPS) {
    ts->deps[ts->dep_count] = {writer, txn_id, from_dirty_read};
    ts->dep_count++;
  }
}

// Check if a dependent txn has finished. Returns:
//   TXN_RUNNING   — still running
//   TXN_COMMITTED — committed
//   TXN_ABORTED   — aborted
//   -1            — finished but status unknown (evicted from ring buffer)
static inline int check_dep_status(Dependency* dep) {
  TxnManState* ws = get_txn_state(dep->writer);
  if (!ws) {
    return -1;
  }
  // Fast path: writer still on the same txn — read status directly.
  if (ws->txn_id == dep->txn_id) {
    return ws->status;
  }
  // Writer moved on. Look up result in ring buffer.
  int status;
  if (lookup_txn_result(ws, dep->txn_id, &status)) {
    return status;
  }
  // Evicted — status unknown.
  return -1;
}

// ---- Wait ----
static const uint64_t WAIT_TIMEOUT = 100000;
static RC do_wait(TxnManState* txn_state, const PolicyEntry* policy) {
  // Wait values come from the policy entry. Per §4.2, if the previous access
  // had early_validation set, this entry's wait values serve as the
  // pre-validation wait (consolidated with the pre-access wait).
  for (int d = 0; d < txn_state->dep_count; d++) {
    Dependency* dep = &txn_state->deps[d];
    // Determine wait target based on dep's txn type. We need the dep's
    // TxnManState to know its type. If the dep already finished, skip.
    int s = check_dep_status(dep);
    if (s != TXN_RUNNING) {
      if ((s == TXN_ABORTED || s == -1) && dep->from_dirty_read) {
        return Abort;
      }
      continue;
    }
    TxnManState* dep_state = (TxnManState*)dep->writer->cc_txn_state;
    if (!dep_state) {
      continue;
    }
    // Policy wait targets are Polyjuice local step values (per-txn-type).
    // Our published steps are a superset of these values (we skip some
    // intermediate steps due to RD+WR consolidation and missing inserts),
    // but since we compare with >=, waiting for a skipped step is satisfied
    // when the next real step is published.
    int target = (dep_state->txn_type == TXN_NEW_ORDER) ? policy->wait_new_order
                                                        : policy->wait_payment;
    if (!target) {
      continue;
    }
    uint64_t t0 = get_sys_clock();
    while (true) {
      s = check_dep_status(dep);
      if (s != TXN_RUNNING) {
        if ((s == TXN_ABORTED || s == -1) && dep->from_dirty_read) {
          return Abort;
        }
        break;
      }
      dep_state = (TxnManState*)dep->writer->cc_txn_state;
      if (!dep_state) {
        break;
      }
      if (dep_state->step >= target) {
        break;
      }
      if (get_sys_clock() - t0 > WAIT_TIMEOUT) {
        return Abort;
      }
      PAUSE
    }
  }
  return RCOK;
}

// ---- Piece validate+expose (NOT install to orig_row) ----
// Validates reads/writes in the current piece and, for PUBLIC writes,
// exposes dirty data so other txns can dirty-read it.
// Per §4.2: "we defer appending reads and visible-writes to their
// corresponding access lists until a successful early-validation."
static RC piece_validate_and_expose(txn_man* txn, TxnManState* txn_state) {
  int pr = txn_state->piece_read_start;
  int pre = txn_state->read_count;
  int pw = txn_state->piece_write_start;
  int pwe = txn_state->write_count;
  // Lock all write rows in this piece
  int locked = 0;
  for (int i = pw; i < pwe; i++) {
    RowState* rs = (RowState*)txn_state->writes[i].orig_row->cc_row_state;
    if (!try_lock(rs)) {
      for (int j = pw; j < pw + locked; j++) {
        unlock((RowState*)txn_state->writes[j].orig_row->cc_row_state);
      }
      return Abort;
    }
    locked++;
  }
  // Validate reads in this piece.
  // Correctness: we check that tid_word hasn't changed since our read.
  // - Clean reads: tid_word change means another txn committed or exposed a
  //   write. This is equivalent to Polyjuice's check for foreign write
  //   entries on the access list, but more conservative (we also abort if
  //   an intervening dirty writer later aborted, resetting the tid).
  // - Dirty reads: validated via dependency tracking (cascading abort if
  //   the writer aborts), so we skip tid_word validation (dirty=true).
  // - Self-writes: if this row is also in our write set, we hold the lock,
  //   so tid_word has LOCK_BIT set. The own_write check handles this.
  for (int i = pr; i < pre; i++) {
    if (txn_state->reads[i].dirty) {
      continue;
    }
    RowState* rs = (RowState*)txn_state->reads[i].row->cc_row_state;
    uint64_t v = rs->tid_word;
    if (v & LOCK_BIT) {
      // Locked by someone — OK if we also write this row (we hold the lock)
      bool own_write = false;
      for (int j = pw; j < pwe; j++) {
        if (txn_state->writes[j].orig_row == txn_state->reads[i].row) {
          own_write = true;
          break;
        }
      }
      if (!own_write) {
        for (int j = pw; j < pw + locked; j++) {
          unlock((RowState*)txn_state->writes[j].orig_row->cc_row_state);
        }
        return Abort;
      }
      v &= ~LOCK_BIT;
    }
    if (txn_state->reads[i].tid != v) {
      // Tid changed — but if we also wrote this row (in a prior piece),
      // our own piece validation may have advanced the tid.
      bool own_write2 = false;
      for (int j = 0; j < txn_state->write_count; j++) {
        if (txn_state->writes[j].orig_row == txn_state->reads[i].row) {
          own_write2 = true;
          break;
        }
      }
      if (!own_write2) {
        for (int j = pw; j < pw + locked; j++) {
          unlock((RowState*)txn_state->writes[j].orig_row->cc_row_state);
        }
        return Abort;
      }
    }
  }
  // Compute commit TID
  uint64_t max_tid = txn_state->commit_tid;
  for (int i = pr; i < pre; i++) {
    if (txn_state->reads[i].tid > max_tid) {
      max_tid = txn_state->reads[i].tid;
    }
  }
  for (int i = pw; i < pwe; i++) {
    uint64_t t =
        get_tid((RowState*)txn_state->writes[i].orig_row->cc_row_state);
    if (t > max_tid) {
      max_tid = t;
    }
  }
  txn_state->commit_tid = max_tid + 1;
  // Expose PUBLIC writes to dirty list; PRIVATE writes stay local.
  // Record write-write deps on prior uncommitted writers regardless.
  for (int i = pw; i < pwe; i++) {
    WriteEntry& w = txn_state->writes[i];
    RowState* rs = (RowState*)w.orig_row->cc_row_state;
    // Add write-write dependencies on prior uncommitted writers
    for (DirtyEntry* e = rs->dirty_head; e; e = e->next) {
      if (e->writer != txn) {
        add_dependency(txn_state, e->writer, e->txn_id, false);
      }
    }
    if (POLICY[w.policy_index].write_visibility == 1) {
      // PUBLIC: expose dirty data so other txns can dirty-read it.
      uint32_t sz = w.orig_row->get_tuple_size();
      DirtyEntry* entry = (DirtyEntry*)malloc(sizeof(DirtyEntry));
      entry->writer = txn;
      entry->txn_id = txn_state->txn_id;
      entry->data = (char*)malloc(sz);
      memcpy(entry->data, w.local_copy->get_data(), sz);
      entry->next = rs->dirty_head;
      rs->dirty_head = entry;
      w.exposed = true;
    }
    // PRIVATE writes: w.exposed remains false, no dirty_head entry.
    rs->tid_word = txn_state->commit_tid | LOCK_BIT;
  }
  for (int i = pw; i < pwe; i++) {
    unlock((RowState*)txn_state->writes[i].orig_row->cc_row_state);
  }
  txn_state->piece_read_start = txn_state->read_count;
  txn_state->piece_write_start = txn_state->write_count;
  return RCOK;
}

// ---- Final commit: validate ALL + copy local→orig atomically ----
static RC final_commit(txn_man* txn, TxnManState* txn_state) {
  // Lock all write rows
  int locked = 0;
  for (int i = 0; i < txn_state->write_count; i++) {
    RowState* rs = (RowState*)txn_state->writes[i].orig_row->cc_row_state;
    if (!try_lock(rs)) {
      for (int j = 0; j < locked; j++) {
        unlock((RowState*)txn_state->writes[j].orig_row->cc_row_state);
      }
      return Abort;
    }
    locked++;
  }
  // Validate all reads (same tid_word approach as piece validation; see
  // comment in piece_validate_and_expose for correctness reasoning).
  for (int i = 0; i < txn_state->read_count; i++) {
    if (txn_state->reads[i].dirty) {
      continue;
    }
    RowState* rs = (RowState*)txn_state->reads[i].row->cc_row_state;
    uint64_t v = rs->tid_word;
    if (v & LOCK_BIT) {
      bool own_write = false;
      for (int j = 0; j < txn_state->write_count; j++) {
        if (txn_state->writes[j].orig_row == txn_state->reads[i].row) {
          own_write = true;
          break;
        }
      }
      if (!own_write) {
        for (int j = 0; j < locked; j++) {
          unlock((RowState*)txn_state->writes[j].orig_row->cc_row_state);
        }
        return Abort;
      }
      v &= ~LOCK_BIT;
    }
    if (txn_state->reads[i].tid != v) {
      // Tid changed — but if we also wrote this row, our own piece
      // validation may have advanced the tid. That's not a conflict.
      bool own_write2 = false;
      for (int j = 0; j < txn_state->write_count; j++) {
        if (txn_state->writes[j].orig_row == txn_state->reads[i].row) {
          own_write2 = true;
          break;
        }
      }
      if (!own_write2) {
        for (int j = 0; j < locked; j++) {
          unlock((RowState*)txn_state->writes[j].orig_row->cc_row_state);
        }
        return Abort;
      }
    }
  }
  // Compute final commit TID
  uint64_t max_tid = txn_state->commit_tid;
  for (int i = 0; i < txn_state->read_count; i++) {
    if (txn_state->reads[i].tid > max_tid) {
      max_tid = txn_state->reads[i].tid;
    }
  }
  for (int i = 0; i < txn_state->write_count; i++) {
    uint64_t t =
        get_tid((RowState*)txn_state->writes[i].orig_row->cc_row_state);
    if (t > max_tid) {
      max_tid = t;
    }
  }
  txn_state->commit_tid = max_tid + 1;
  // Install: local_copy → orig_row, remove our entry from dirty list
  for (int i = 0; i < txn_state->write_count; i++) {
    WriteEntry& w = txn_state->writes[i];
    RowState* rs = (RowState*)w.orig_row->cc_row_state;
    uint32_t sz = w.orig_row->get_tuple_size();
    memcpy(w.orig_row->get_data(), w.local_copy->get_data(), sz);
    rs->tid_word = txn_state->commit_tid | LOCK_BIT;
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
  for (int i = 0; i < txn_state->write_count; i++) {
    unlock((RowState*)txn_state->writes[i].orig_row->cc_row_state);
  }
  txn_state->status = TXN_COMMITTED;
  txn_state->step = (txn_state->txn_type == TXN_NEW_ORDER) ? 11 : 7;
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

// ---- Adaptive backoff (per the Polyjuice paper §4.3) ----
// On abort, increase backoff; on commit, decrease. Spin before retrying.
// ABORT_PENALTY should be 0 when using this variant.
static constexpr uint64_t BACKOFF_MIN = 1000;     // ~1 us at 1 GHz
static constexpr uint64_t BACKOFF_MAX = 100000;   // ~100 us
static thread_local uint64_t pj_backoff[2] = {};  // [0]=new_order [1]=payment

void cc_pre_txn(thread_t* th, txn_man* tx, base_query* q) {
  (void)th;
  tpcc_query* tq = (tpcc_query*)q;
  TpccTxnType txn_type =
      (tq->type == TPCC_NEW_ORDER) ? TXN_NEW_ORDER : TXN_PAYMENT;
  // Adaptive backoff: spin before starting if previous attempt aborted.
  if (pj_backoff[txn_type] > 0) {
    uint64_t t0 = get_sys_clock();
    while (get_sys_clock() - t0 < pj_backoff[txn_type]) {
      PAUSE
    }
  }
  // Reuse TxnManState across transactions; allocate on first use.
  TxnManState* ts = get_txn_state(tx);
  if (!ts) {
    ts = (TxnManState*)_mm_malloc(sizeof(TxnManState), 64);
    new (ts) TxnManState{};
    tx->cc_txn_state = ts;
  }
  // Increment persistent txn_id; reset per-txn fields.
  ts->txn_id = ts->txn_id + 1;
  ts->txn_type = txn_type;
  ts->ol_cnt = (txn_type == TXN_NEW_ORDER) ? tq->ol_cnt : 0;
  ts->step = 0;
  ts->status = TXN_RUNNING;
  ts->commit_tid = 0;
  ts->dep_count = 0;
  ts->read_count = 0;
  ts->write_count = 0;
  ts->piece_read_start = 0;
  ts->piece_write_start = 0;
  ts->pending_piece_validation = false;
}

void cc_post_txn(thread_t* th, txn_man* tx, RC r) {
  (void)th;
  TxnManState* ts = (TxnManState*)tx->cc_txn_state;
  if (!ts) {
    return;
  }
  // Adjust backoff: increase on abort, decrease on commit.
  if (r == RCOK) {
    pj_backoff[ts->txn_type] /= 2;
  } else {
    uint64_t b = pj_backoff[ts->txn_type];
    pj_backoff[ts->txn_type] =
        (b == 0) ? BACKOFF_MIN : (b * 2 < BACKOFF_MAX ? b * 2 : BACKOFF_MAX);
  }
  // Publish outcome to ring buffer. TxnManState is NOT freed — reused next txn.
  int final_status = (r == RCOK) ? TXN_COMMITTED : TXN_ABORTED;
  publish_txn_result(ts, final_status);
  for (int i = 0; i < ts->write_count; i++) {
    if (ts->writes[i].local_copy) {
      ts->writes[i].local_copy->free_row();
      _mm_free(ts->writes[i].local_copy);
    }
  }
}

RC cc_pre_op(txn_man* txn, row_t* orig, access_t type, int op) {
  TxnManState* ts = (TxnManState*)txn->cc_txn_state;
  RowState* rs = (RowState*)orig->cc_row_state;
  // If previous access had early_validation, do piece validation now
  if (ts->pending_piece_validation) {
    ts->pending_piece_validation = false;
    if (piece_validate_and_expose(txn, ts) != RCOK) {
      ts->status = TXN_ABORTED;
      return Abort;
    }
  }
  if (ts->status == TXN_ABORTED) {
    return Abort;
  }
  int step;
  const PolicyEntry* policy =
      lookup_policy(ts->txn_type, op, ts->ol_cnt, &step);
  if (do_wait(ts, policy) != RCOK) {
    return Abort;
  }
  if (type == RD || type == SCAN) {
    if (policy->read_version == 1) {
      // DIRTY_READ: read latest uncommitted version from dirty list
      spin_lock(rs);
      DirtyEntry* de = rs->dirty_head;
      // Find the latest entry from a different, still-running txn
      while (de && (de->writer == txn)) {
        de = de->next;
      }
      if (de) {
        TxnManState* writer_state = (TxnManState*)de->writer->cc_txn_state;
        if (writer_state && writer_state->status == TXN_RUNNING) {
          add_dependency(ts, de->writer, de->txn_id, true);
          uint64_t tid = get_tid(rs);
          unlock(rs);
          if (ts->read_count < MAX_ACCESSES) {
            ts->reads[ts->read_count] = {orig, tid, true};
            ts->read_count++;
          }
          if (step > ts->step) {
            ts->step = step;
          }
          return RCOK;
        }
      }
      // No valid dirty data available — fall back to clean read
      uint64_t tid = get_tid(rs);
      unlock(rs);
      if (ts->read_count < MAX_ACCESSES) {
        ts->reads[ts->read_count] = {orig, tid, false};
        ts->read_count++;
      }
    } else {
      // CLEAN_READ: read latest committed version (Silo-style)
      uint64_t v, v2 = ~0ULL;
      do {
        v = rs->tid_word;
        while (v & LOCK_BIT) {
          PAUSE;
          v = rs->tid_word;
        }
        COMPILER_BARRIER
        v2 = rs->tid_word;
      } while (v != v2);
      if (ts->read_count < MAX_ACCESSES) {
        ts->reads[ts->read_count] = {orig, v & ~LOCK_BIT, false};
        ts->read_count++;
      }
    }
  }
  if (step > ts->step) {
    ts->step = step;
  }
  return RCOK;
}

void cc_post_op(txn_man* txn, row_t* orig, row_t** local_row_out, access_t type,
                int op) {
  TxnManState* ts = (TxnManState*)txn->cc_txn_state;
  RowState* rs = (RowState*)orig->cc_row_state;
  int step;
  const PolicyEntry* policy =
      lookup_policy(ts->txn_type, op, ts->ol_cnt, &step);
  if (type == RD || type == SCAN) {
    // For dirty reads: copy dirty data into a local row for the txn to use
    if (policy->read_version == 1 && ts->read_count > 0 &&
        ts->reads[ts->read_count - 1].dirty) {
      spin_lock(rs);
      // Find the latest dirty entry from a different txn
      DirtyEntry* de = rs->dirty_head;
      while (de && de->writer == txn) {
        de = de->next;
      }
      if (de && de->data) {
        uint32_t sz = orig->get_tuple_size();
        row_t* copy = (row_t*)_mm_malloc(sizeof(row_t), 64);
        copy->init(orig->get_table(), orig->get_part_id());
        memcpy(copy->get_data(), de->data, sz);
        unlock(rs);
        *local_row_out = copy;
        txn->accesses[txn->row_cnt]->data = copy;
      } else {
        unlock(rs);
      }
    }
  }
  if (type == WR) {
    // Writes always go to a local copy; orig_row is untouched until commit
    row_t* local_copy = (row_t*)_mm_malloc(sizeof(row_t), 64);
    local_copy->init(orig->get_table(), orig->get_part_id());
    local_copy->copy(orig);
    *local_row_out = local_copy;
    txn->accesses[txn->row_cnt]->data = local_copy;
    int pidx = static_cast<int>(policy - POLICY);
    if (ts->write_count < MAX_ACCESSES) {
      ts->writes[ts->write_count] = {orig, local_copy, pidx, false};
      ts->write_count++;
    }
    if (ts->read_count < MAX_ACCESSES) {
      ts->reads[ts->read_count] = {orig, get_tid(rs), false};
      ts->read_count++;
    }
  }
  if (policy->early_validation) {
    ts->pending_piece_validation = true;
  }
}

RC cc_pre_commit(txn_man* txn) {
  TxnManState* ts = (TxnManState*)txn->cc_txn_state;
  if (ts->status == TXN_ABORTED) {
    return Abort;
  }
  // Flush any pending piece validation
  if (ts->pending_piece_validation) {
    ts->pending_piece_validation = false;
    if (piece_validate_and_expose(txn, ts) != RCOK) {
      return Abort;
    }
  }
  // Wait for all dependencies to finish
  for (int d = 0; d < ts->dep_count; d++) {
    Dependency* dep = &ts->deps[d];
    uint64_t t0 = get_sys_clock();
    while (true) {
      int s = check_dep_status(dep);
      if (s != TXN_RUNNING) {
        if ((s == TXN_ABORTED || s == -1) && dep->from_dirty_read) {
          return Abort;
        }
        break;
      }
      if (get_sys_clock() - t0 > WAIT_TIMEOUT * 10) {
        return Abort;
      }
      PAUSE
    }
  }
  return final_commit(txn, ts);
}

void cc_release_op(txn_man* txn, row_t* orig, row_t* local, access_t type,
                   int op) {
  (void)op;
  TxnManState* ts = (TxnManState*)txn->cc_txn_state;
  if (type == XP) {
    // Abort path: remove our entry from dirty list
    RowState* rs = (RowState*)orig->cc_row_state;
    spin_lock(rs);
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
    unlock(rs);
    ts->status = TXN_ABORTED;
  }
  if ((type == RD || type == SCAN) && local && local != orig) {
    local->free_row();
    _mm_free(local);
  }
}
