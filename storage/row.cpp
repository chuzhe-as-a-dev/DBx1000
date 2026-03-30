#include "row.h"

#include <mm_malloc.h>

#include "catalog.h"
#include "cc_hooks.h"
#include "global.h"
#include "manager.h"
#include "mem_alloc.h"
#include "row_hekaton.h"
#include "row_lock.h"
#include "row_mvcc.h"
#include "row_occ.h"
#include "row_silo.h"
#include "row_tictoc.h"
#include "row_ts.h"
#include "row_vll.h"
#include "table.h"
#include "txn.h"

// Cast helpers: each CC algorithm accesses cc_row_state through its own typed
// pointer.
#if CC_ALG == DL_DETECT || CC_ALG == NO_WAIT || CC_ALG == WAIT_DIE
static inline Row_lock* cc_mgr(row_t* r) { return (Row_lock*)r->cc_row_state; }
#elif CC_ALG == TIMESTAMP
static inline Row_ts* cc_mgr(row_t* r) { return (Row_ts*)r->cc_row_state; }
#elif CC_ALG == MVCC
static inline Row_mvcc* cc_mgr(row_t* r) { return (Row_mvcc*)r->cc_row_state; }
#elif CC_ALG == HEKATON
static inline Row_hekaton* cc_mgr(row_t* r) {
  return (Row_hekaton*)r->cc_row_state;
}
#elif CC_ALG == OCC
static inline Row_occ* cc_mgr(row_t* r) { return (Row_occ*)r->cc_row_state; }
#elif CC_ALG == TICTOC
static inline Row_tictoc* cc_mgr(row_t* r) {
  return (Row_tictoc*)r->cc_row_state;
}
#elif CC_ALG == SILO
static inline Row_silo* cc_mgr(row_t* r) { return (Row_silo*)r->cc_row_state; }
#elif CC_ALG == VLL
static inline Row_vll* cc_mgr(row_t* r) { return (Row_vll*)r->cc_row_state; }
#endif

RC row_t::init(table_t* host_table, uint64_t part_id, uint64_t row_id) {
  _row_id = row_id;
  _part_id = part_id;
  this->table = host_table;
  Catalog* schema = host_table->get_schema();
  int tuple_size = schema->get_tuple_size();
  data = (char*)_mm_malloc(sizeof(char) * tuple_size, 64);
  return RCOK;
}
void row_t::init(int size) { data = (char*)_mm_malloc(size, 64); }

RC row_t::switch_schema(table_t* host_table) {
  this->table = host_table;
  return RCOK;
}

// Allocates and initialises the per-row CC manager whose type is chosen
// entirely at compile time by CC_ALG. The manager lives adjacent to the row
// (or in the thread-local arena) and holds all algorithm-specific metadata
// (lock lists, version chains, timestamp words, etc.).
// HSTORE has no per-row manager — partition locks are used instead.
// (AI-generated)
void row_t::init_manager(row_t* row) {
#if CC_ALG == PER_OP
  cc_init_row_state(this);
  (void)row;
  return;
#elif CC_ALG == DL_DETECT || CC_ALG == NO_WAIT || CC_ALG == WAIT_DIE
  cc_row_state = (Row_lock*)mem_allocator.alloc(sizeof(Row_lock), _part_id);
#elif CC_ALG == TIMESTAMP
  cc_row_state = (Row_ts*)mem_allocator.alloc(sizeof(Row_ts), _part_id);
#elif CC_ALG == MVCC
  cc_row_state = (Row_mvcc*)_mm_malloc(sizeof(Row_mvcc), 64);
#elif CC_ALG == HEKATON
  cc_row_state = (Row_hekaton*)_mm_malloc(sizeof(Row_hekaton), 64);
#elif CC_ALG == OCC
  cc_row_state = (Row_occ*)mem_allocator.alloc(sizeof(Row_occ), _part_id);
#elif CC_ALG == TICTOC
  cc_row_state = (Row_tictoc*)_mm_malloc(sizeof(Row_tictoc), 64);
#elif CC_ALG == SILO
  cc_row_state = (Row_silo*)_mm_malloc(sizeof(Row_silo), 64);
#elif CC_ALG == VLL
  cc_row_state = (Row_vll*)mem_allocator.alloc(sizeof(Row_vll), _part_id);
#endif

#if CC_ALG != HSTORE && CC_ALG != PER_OP
  cc_mgr(this)->init(this);
#endif
}

table_t* row_t::get_table() { return table; }

Catalog* row_t::get_schema() { return get_table()->get_schema(); }

const char* row_t::get_table_name() { return get_table()->get_table_name(); };
uint64_t row_t::get_tuple_size() { return get_schema()->get_tuple_size(); }

uint64_t row_t::get_field_cnt() { return get_schema()->field_cnt; }

void row_t::set_value(int id, void* ptr) {
  int datasize = get_schema()->get_field_size(id);
  int pos = get_schema()->get_field_index(id);
  memcpy(&data[pos], ptr, datasize);
}

void row_t::set_value(int id, void* ptr, int size) {
  int pos = get_schema()->get_field_index(id);
  memcpy(&data[pos], ptr, size);
}

void row_t::set_value(const char* col_name, void* ptr) {
  uint64_t id = get_schema()->get_field_id(col_name);
  set_value(id, ptr);
}

SET_VALUE(uint64_t);
SET_VALUE(int64_t);
SET_VALUE(double);
SET_VALUE(UInt32);
SET_VALUE(SInt32);

GET_VALUE(uint64_t);
GET_VALUE(int64_t);
GET_VALUE(double);
GET_VALUE(UInt32);
GET_VALUE(SInt32);

char* row_t::get_value(int id) {
  int pos = get_schema()->get_field_index(id);
  return &data[pos];
}

char* row_t::get_value(char* col_name) {
  uint64_t pos = get_schema()->get_field_index(col_name);
  return &data[pos];
}

char* row_t::get_data() { return data; }

void row_t::set_data(char* data, uint64_t size) {
  memcpy(this->data, data, size);
}
// copy from the src to this
void row_t::copy(row_t* src) {
  set_data(src->get_data(), src->get_tuple_size());
}

void row_t::free_row() { _mm_free(data); }

// Acquires access to a row, dispatching to the compiled-in CC algorithm.
//
// Lock-based (WAIT_DIE / NO_WAIT / DL_DETECT):
//   Requests SH or EX lock. On WAIT, spins until lock_ready or lock_abort.
//   DL_DETECT additionally registers dependencies and runs cycle detection
//   while waiting; on timeout (g_timeout) it self-aborts.
//
// Timestamp-based (TIMESTAMP / MVCC / HEKATON):
//   Calls manager->access() which may return WAIT if a prewrite is pending;
//   the caller spins on ts_ready.
//
// Optimistic (OCC / TICTOC / SILO):
//   Always succeeds immediately; conflict detection is deferred to commit.
//   A local copy is made here so the txn works on private data.
//
// HSTORE / VLL: no per-row locking; return the row directly. (AI-generated)
RC row_t::get_row(access_t type, txn_man* txn, row_t*& row, int op_idx) {
  RC rc = RCOK;
#if CC_ALG == WAIT_DIE || CC_ALG == NO_WAIT || CC_ALG == DL_DETECT
  uint64_t thd_id = txn->get_thd_id();
  lock_t lt = (type == RD || type == SCAN) ? LOCK_SH : LOCK_EX;
#if CC_ALG == DL_DETECT
  uint64_t* txnids;
  int txncnt;
  rc = cc_mgr(this)->lock_get(lt, txn, txnids, txncnt);
#else
  rc = cc_mgr(this)->lock_get(lt, txn);
#endif

  if (rc == RCOK) {
    row = this;
  } else if (rc == Abort) {
  } else if (rc == WAIT) {
    ASSERT(CC_ALG == WAIT_DIE || CC_ALG == DL_DETECT);
    uint64_t starttime = get_sys_clock();
#if CC_ALG == DL_DETECT
    bool dep_added = false;
#endif
    uint64_t endtime;
    txn->lock_abort = false;
    INC_STATS(txn->get_thd_id(), wait_cnt, 1);
    while (!txn->lock_ready && !txn->lock_abort) {
#if CC_ALG == WAIT_DIE
      continue;
#elif CC_ALG == DL_DETECT
      uint64_t last_detect = starttime;
      uint64_t last_try = starttime;

      uint64_t now = get_sys_clock();
      if (now - starttime > g_timeout) {
        txn->lock_abort = true;
        break;
      }
      if (g_no_dl) {
        PAUSE
        continue;
      }
      int ok = 0;
      if ((now - last_detect > g_dl_loop_detect) &&
          (now - last_try > DL_LOOP_TRIAL)) {
        if (!dep_added) {
          ok = dl_detector.add_dep(txn->get_txn_id(), txnids, txncnt,
                                   txn->row_cnt);
          if (ok == 0)
            dep_added = true;
          else if (ok == 16)
            last_try = now;
        }
        if (dep_added) {
          ok = dl_detector.detect_cycle(txn->get_txn_id());
          if (ok == 16)  // failed to lock the deadlock detector
            last_try = now;
          else if (ok == 0)
            last_detect = now;
          else if (ok == 1) {
            last_detect = now;
          }
        }
      } else
        PAUSE
#endif
    }
    if (txn->lock_ready)
      rc = RCOK;
    else if (txn->lock_abort) {
      rc = Abort;
      return_row(type, txn, NULL);
    }
    endtime = get_sys_clock();
    INC_TMP_STATS(thd_id, time_wait, endtime - starttime);
    row = this;
  }
  return rc;
#elif CC_ALG == TIMESTAMP || CC_ALG == MVCC || CC_ALG == HEKATON
  uint64_t thd_id = txn->get_thd_id();
  // For TIMESTAMP RD, a new copy of the row will be returned.
  // for MVCC RD, the version will be returned instead of a copy
  // So for MVCC RD-WR, the version should be explicitly copied.
  // row_t * newr = NULL;
#if CC_ALG == TIMESTAMP
  // TODO. should not call malloc for each row read. Only need to call malloc
  // once before simulation starts, like TicToc and Silo.
  txn->cur_row =
      (row_t*)mem_allocator.alloc(sizeof(row_t), this->get_part_id());
  txn->cur_row->init(get_table(), this->get_part_id());
#endif

  // TODO need to initialize the table/catalog information.
  TsType ts_type = (type == RD) ? R_REQ : P_REQ;
  rc = cc_mgr(this)->access(txn, ts_type, row);
  if (rc == RCOK) {
    row = txn->cur_row;
  } else if (rc == WAIT) {
    uint64_t t1 = get_sys_clock();
    while (!txn->ts_ready) PAUSE
    uint64_t t2 = get_sys_clock();
    INC_TMP_STATS(thd_id, time_wait, t2 - t1);
    row = txn->cur_row;
  }
  if (rc != Abort) {
    row->table = get_table();
    assert(row->get_schema() == this->get_schema());
  }
  return rc;
#elif CC_ALG == OCC
  // OCC always make a local copy regardless of read or write
  txn->cur_row = (row_t*)mem_allocator.alloc(sizeof(row_t), get_part_id());
  txn->cur_row->init(get_table(), get_part_id());
  rc = cc_mgr(this)->access(txn, R_REQ);
  row = txn->cur_row;
  return rc;
#elif CC_ALG == TICTOC || CC_ALG == SILO
  // like OCC, tictoc also makes a local copy for each read/write
  row->table = get_table();
  TsType ts_type = (type == RD) ? R_REQ : P_REQ;
  rc = cc_mgr(this)->access(txn, ts_type, row);
  return rc;
#elif CC_ALG == HSTORE || CC_ALG == VLL
  row = this;
  return rc;
#elif CC_ALG == PER_OP
  rc = cc_pre_op(txn, this, type, op_idx);
  if (rc != RCOK) return rc;
  row = this;
  return RCOK;
#else
  assert(false);
#endif
}

// the "row" is the row read out in get_row().
// For locking based CC_ALG, the "row" is the same as "this".
// For timestamp based CC_ALG, the "row" != "this", and the "row" must be freed.
// For MVCC, the row will simply serve as a version. The version will be
// delete during history cleanup.
// For TIMESTAMP, the row will be explicity deleted at the end of access().
// (cf. row_ts.cpp)
void row_t::return_row(access_t type, txn_man* txn, row_t* row, int op_idx) {
#if CC_ALG == WAIT_DIE || CC_ALG == NO_WAIT || CC_ALG == DL_DETECT
  assert(row == NULL || row == this || type == XP);
  if (ROLL_BACK && type == XP) {  // recover from previous writes.
    this->copy(row);
  }
  cc_mgr(this)->lock_release(txn);
#elif CC_ALG == TIMESTAMP || CC_ALG == MVCC
  // for RD or SCAN or XP, the row should be deleted.
  // because all WR should be companied by a RD
  // for MVCC RD, the row is not copied, so no need to free.
#if CC_ALG == TIMESTAMP
  if (type == RD || type == SCAN) {
    row->free_row();
    mem_allocator.free(row, sizeof(row_t));
  }
#endif
  if (type == XP) {
    cc_mgr(this)->access(txn, XP_REQ, row);
  } else if (type == WR) {
    assert(type == WR && row != NULL);
    assert(row->get_schema() == this->get_schema());
    RC rc = cc_mgr(this)->access(txn, W_REQ, row);
    assert(rc == RCOK);
  }
#elif CC_ALG == OCC
  assert(row != NULL);
  if (type == WR) cc_mgr(this)->write(row, txn->end_ts);
  row->free_row();
  mem_allocator.free(row, sizeof(row_t));
  return;
#elif CC_ALG == TICTOC || CC_ALG == SILO
  assert(row != NULL);
  return;
#elif CC_ALG == HSTORE || CC_ALG == VLL
  return;
#elif CC_ALG == PER_OP
  cc_release_op(txn, this, row, type, op_idx);
  return;
#else
  assert(false);
#endif
}
