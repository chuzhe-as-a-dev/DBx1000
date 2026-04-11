// cc_hooks_pj_tpcc_4wh.cpp — Faithful Polyjuice learned policy.
// Dirty reads, dependency tracking, piece boundaries, atomic final commit.
// orig_row NEVER modified until final commit. Writes go to local copies.

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
  uint64_t txn_id;  // writer's TxnManState::txn_id when dep was created
  TpccTxnType dep_txn_type;  // writer's txn type, captured at creation time
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
  bool to_expose;  // true if write_visibility == PUBLIC (expose at validation)
  bool exposed;    // true if dirty data has been exposed via early-validation
};
#define MAX_ACCESSES 64
#define MAX_DEPS 64
struct TxnManState {
  // -- Per-txn fields (reset each transaction) --
  TpccTxnType txn_type;
  int ol_cnt;         // order-line count (new_order only)
  volatile int step;  // current progress for wait tracking
  volatile TxnStatus status;
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
  // Monotonically increasing id, incremented each cc_pre_txn. Starts at 0
  // and is bumped to 1 before the first transaction, so valid txn_ids are
  // always >= 1. This makes 0 a safe sentinel for uninitialized ring buffer
  // slots (see TxnResult below).
  volatile uint64_t txn_id = 0;
  // Ring buffer of recent completed txn outcomes (seqlock protocol).
  // Slot validity: txn_id == 0 means uninitialized (never written),
  // txn_id == HISTORY_UPDATING means write in progress, otherwise valid.
  // Readers skip slots where txn_id doesn't match the target.
  struct TxnResult {
    volatile uint64_t txn_id;  // 0=uninitialized, UPDATING=write in progress
    volatile TxnStatus status;
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
static inline void publish_txn_result(TxnManState* tms, TxnStatus status) {
  int slot = tms->history_head;
  tms->history[slot].txn_id = TxnManState::HISTORY_UPDATING;
  COMPILER_BARRIER
  tms->history[slot].status = status;
  COMPILER_BARRIER
  tms->history[slot].txn_id = tms->txn_id;
  tms->history_head = (slot + 1) % TxnManState::HISTORY_SIZE;
}

// Look up a txn's outcome in the writer's ring buffer.
static inline bool lookup_txn_result(TxnManState* writer_tms, uint64_t txn_id,
                                     TxnStatus* status) {
  for (int i = 0; i < TxnManState::HISTORY_SIZE; i++) {
    uint64_t id1;

    // If write in progress, spin until it completes, then check.
    while ((id1 = writer_tms->history[i].txn_id) ==
           TxnManState::HISTORY_UPDATING) {
      PAUSE
    }
    if (id1 == 0 || id1 != txn_id) {  // 0 = unused
      continue;
    }

    COMPILER_BARRIER
    TxnStatus s = writer_tms->history[i].status;
    COMPILER_BARRIER
    uint64_t id2 = writer_tms->history[i].txn_id;
    if (id1 == id2) {
      *status = s;
      return true;
    }
  }
  return false;
}

// Add a dependency, deduplicating by (writer, txn_id). If an existing dep
// with from_dirty_read=false is found and the new one is true, upgrade it.
// Returns false if the dep array is full (caller should abort).
static inline bool add_dependency(TxnManState* tms, txn_man* writer,
                                  uint64_t txn_id, TpccTxnType dep_txn_type,
                                  bool from_dirty_read) {
  for (int i = 0; i < tms->dep_count; i++) {
    if (tms->deps[i].writer == writer && tms->deps[i].txn_id == txn_id) {
      if (from_dirty_read) {
        tms->deps[i].from_dirty_read = true;
      }
      return true;
    }
  }
  if (tms->dep_count >= MAX_DEPS) {
    return false;
  }
  tms->deps[tms->dep_count] = {writer, txn_id, dep_txn_type, from_dirty_read};
  tms->dep_count++;
  return true;
}

static inline TxnStatus check_dep_status(Dependency* dep) {
  TxnManState* ws = get_tms(dep->writer);
  assert(ws);  // TxnManState is never freed; null means dep on a txn_man
  // that never ran a transaction — should not happen.

  if (ws->txn_id == dep->txn_id) {
    return TXN_RUNNING;
  }

  // Ring buffer is the authoritative source for completed txn outcomes.
  TxnStatus status;
  if (lookup_txn_result(ws, dep->txn_id, &status)) {
    return status;
  }

  // Not in ring buffer: either still running, or evicted.
  return TXN_UNKNOWN;
}

// ---- Wait ----
static const uint64_t WAIT_TIMEOUT = 100000;
static RC do_wait(TxnManState* tms, const PolicyEntry* policy) {
  // Wait values come from the policy entry. Per §4.2, if the previous access
  // had early_validation set, this entry's wait values serve as the
  // pre-validation wait (consolidated with the pre-access wait).
  for (int d = 0; d < tms->dep_count; d++) {
    Dependency* dep = &tms->deps[d];
    // Policy wait targets are Polyjuice local step values (per-txn-type).
    // Use dep_txn_type captured at dependency creation to avoid racing with
    // the writer starting a new txn of a different type.
    int target = (dep->dep_txn_type == TXN_NEW_ORDER) ? policy->wait_new_order
                                                      : policy->wait_payment;
    if (!target) {
      continue;
    }

    uint64_t t0 = get_sys_clock();
    while (true) {
      TxnStatus s = check_dep_status(dep);
      if (s != TXN_RUNNING) {
        if ((s == TXN_ABORTED || s == TXN_UNKNOWN) && dep->from_dirty_read) {
          return Abort;
        }
        break;
      }
      if (get_tms(dep->writer)->step >= target) {
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
// Returns true if all reads are still valid.
static bool validate_reads(TxnManState* tms, int r_begin, int r_end,
                           int w_begin, int w_end) {
  for (int i = r_begin; i < r_end; i++) {
    if (tms->reads[i].dirty) {
      continue;
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
          !add_dependency(tms, e->writer, e->txn_id,
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
      entry->txn_id = tms->txn_id;
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
  if (!validate_reads(tms, 0, tms->read_count, 0, tms->write_count)) {
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
  tms->status = TXN_COMMITTED;
  tms->step = (tms->txn_type == TXN_NEW_ORDER) ? 11 : 7;
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
// On abort: backoff *= (1 + x * alpha), then spin for `backoff` nop_pauses.
// On commit: backoff /= (1 + x * alpha), no spin.
// x is a per-(success/failure, retry_count, txn_type) learned double.
// ABORT_PENALTY should be 0 when using this variant.

// Learned multipliers from 48th-4wh.txt policy file.
// Dimensions: [2][3][2] = [success(1)/failure(0)][retry 0,1,>=2][NO,PAY]
// Type 0 in Polyjuice is read-only (always nop); we only have NO and PAY.
static constexpr double BACKOFF_MULT[2][3][2] = {
    // [0] = failure (increase backoff)
    {{8, 1}, {8, 1}, {0, 0}},
    // [1] = success (decrease backoff)
    {{1, 0}, {1, 4}, {4, 1}},
};
static constexpr double BACKOFF_ALPHA = 0.5;
static constexpr uint64_t BACKOFF_FLOOR = 100;
static constexpr uint64_t BACKOFF_CAP = 6710886400ULL;

struct BackoffState {
  uint64_t backoff = BACKOFF_FLOOR;
  int retry_count = 0;
};
static thread_local BackoffState pj_backoff[2];  // [TXN_NEW_ORDER, TXN_PAYMENT]

static inline void adjust_backoff(BackoffState& bs, TpccTxnType type,
                                  bool success) {
  int retry = bs.retry_count > 2 ? 2 : bs.retry_count;
  double x = BACKOFF_MULT[success ? 1 : 0][retry][type];
  double factor = 1.0 + x * BACKOFF_ALPHA;
  if (success) {
    bs.backoff = static_cast<uint64_t>(bs.backoff / factor);
    if (bs.backoff < BACKOFF_FLOOR) bs.backoff = BACKOFF_FLOOR;
    bs.retry_count = 0;
  } else {
    bs.backoff = static_cast<uint64_t>(bs.backoff * factor);
    if (bs.backoff > BACKOFF_CAP) bs.backoff = BACKOFF_CAP;
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
    while (spins--) PAUSE
  }
  // TxnManState allocated in cc_init_txn_man, reused across transactions.
  TxnManState* tms = get_tms(tx);
  assert(tms);
  // Increment persistent txn_id; reset per-txn fields.
  tms->txn_id = tms->txn_id + 1;
  tms->txn_type = txn_type;
  tms->ol_cnt = (txn_type == TXN_NEW_ORDER) ? tq->ol_cnt : 0;
  tms->step = 0;
  tms->status = TXN_RUNNING;

  tms->dep_count = 0;
  tms->read_count = 0;
  tms->write_count = 0;
  tms->piece_read_start = 0;
  tms->piece_write_start = 0;
  tms->pending_piece_validation = false;
}

void cc_post_txn(thread_t* th, txn_man* tx, RC r) {
  (void)th;
  TxnManState* tms = (TxnManState*)tx->cc_txn_state;
  if (!tms) {
    return;
  }
  adjust_backoff(pj_backoff[tms->txn_type], tms->txn_type, r == RCOK);
  // Publish outcome to ring buffer. TxnManState is NOT freed — reused next txn.
  TxnStatus final_status = (r == RCOK) ? TXN_COMMITTED : TXN_ABORTED;
  publish_txn_result(tms, final_status);
  for (int i = 0; i < tms->write_count; i++) {
    if (tms->writes[i].local_copy) {
      tms->writes[i].local_copy->free_row();
      _mm_free(tms->writes[i].local_copy);
    }
  }
}

RC cc_pre_op(txn_man* txn, row_t* orig, access_t type, int op) {
  TxnManState* tms = (TxnManState*)txn->cc_txn_state;
  RowState* rs = (RowState*)orig->cc_row_state;
  // If previous access had early_validation, do piece validation now
  if (tms->pending_piece_validation) {
    tms->pending_piece_validation = false;
    if (piece_validate_and_expose(txn) != RCOK) {
      tms->status = TXN_ABORTED;
      return Abort;
    }
  }
  if (tms->status == TXN_ABORTED) {
    return Abort;
  }
  int step;
  const PolicyEntry* policy =
      lookup_policy(tms->txn_type, op, tms->ol_cnt, &step);
  if (do_wait(tms, policy) != RCOK) {
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
          if (!add_dependency(tms, de->writer, de->txn_id,
                              writer_state->txn_type, true)) {
            unlock(rs);
            return Abort;
          }
          uint64_t tid = get_tid(rs);
          unlock(rs);
          if (tms->read_count < MAX_ACCESSES) {
            tms->reads[tms->read_count] = {orig, tid, true};
            tms->read_count++;
          }
          if (step > tms->step) {
            tms->step = step;
          }
          return RCOK;
        }
      }
      // No valid dirty data available — fall back to clean read
      uint64_t tid = get_tid(rs);
      unlock(rs);
      if (tms->read_count < MAX_ACCESSES) {
        tms->reads[tms->read_count] = {orig, tid, false};
        tms->read_count++;
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
      if (tms->read_count < MAX_ACCESSES) {
        tms->reads[tms->read_count] = {orig, v & ~LOCK_BIT, false};
        tms->read_count++;
      }
    }
  }
  if (step > tms->step) {
    tms->step = step;
  }
  return RCOK;
}

void cc_post_op(txn_man* txn, row_t* orig, row_t** local_row_out, access_t type,
                int op) {
  TxnManState* tms = (TxnManState*)txn->cc_txn_state;
  RowState* rs = (RowState*)orig->cc_row_state;
  int step;
  const PolicyEntry* policy =
      lookup_policy(tms->txn_type, op, tms->ol_cnt, &step);
  if (type == RD || type == SCAN) {
    // For dirty reads: copy dirty data into a local row for the txn to use
    if (policy->read_version == 1 && tms->read_count > 0 &&
        tms->reads[tms->read_count - 1].dirty) {
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
    bool to_expose = (policy->write_visibility == 1);
    if (tms->write_count < MAX_ACCESSES) {
      tms->writes[tms->write_count] = {orig, local_copy, to_expose, false};
      tms->write_count++;
    }
    if (tms->read_count < MAX_ACCESSES) {
      tms->reads[tms->read_count] = {orig, get_tid(rs), false};
      tms->read_count++;
    }
  }
  if (policy->early_validation) {
    tms->pending_piece_validation = true;
  }
}

RC cc_pre_commit(txn_man* txn) {
  TxnManState* tms = (TxnManState*)txn->cc_txn_state;
  if (tms->status == TXN_ABORTED) {
    return Abort;
  }
  // Flush any pending piece validation
  if (tms->pending_piece_validation) {
    tms->pending_piece_validation = false;
    if (piece_validate_and_expose(txn) != RCOK) {
      return Abort;
    }
  }
  // Wait for all dependencies to finish
  for (int d = 0; d < tms->dep_count; d++) {
    Dependency* dep = &tms->deps[d];
    uint64_t t0 = get_sys_clock();
    while (true) {
      TxnStatus s = check_dep_status(dep);
      if (s != TXN_RUNNING) {
        if ((s == TXN_ABORTED || s == TXN_UNKNOWN) && dep->from_dirty_read) {
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
  return final_commit(txn);
}

void cc_release_op(txn_man* txn, row_t* orig, row_t* local, access_t type,
                   int op) {
  (void)op;
  TxnManState* tms = (TxnManState*)txn->cc_txn_state;
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
    tms->status = TXN_ABORTED;
  }
  if ((type == RD || type == SCAN) && local && local != orig) {
    local->free_row();
    _mm_free(local);
  }
}
