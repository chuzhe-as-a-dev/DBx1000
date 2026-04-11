// cc_hooks_mvcc.cpp — MVCC implementation of the cc_hooks interface.
//
// Per-row state: version history (fixed-size, with GC).
// cc_pre_op(WR): P_REQ — reserve a version slot; GC oldest if full.
// cc_pre_op(RD): noop (deferred to cc_post_op).
// cc_post_op(RD): version selection + copy.
// cc_post_op(WR): point local at reserved write buffer.
// cc_pre_commit: noop (conflicts caught at P_REQ/R_REQ time).
// cc_release_op(WR): install version on commit, release slot on abort (XP).
// cc_release_op(RD): free local copy.

#include <mm_malloc.h>
#include <pthread.h>

#include <cstdlib>
#include <cstring>

#include "cc_hooks.h"
#include "manager.h"
#include "mem_alloc.h"
#include "row.h"
#include "thread.h"
#include "txn.h"

#define MVCC_HIS_LEN 8

struct MvccWriteEntry {
  bool valid;
  bool reserved;
  ts_t ts;
  row_t* row;
};

struct MvccRowState {
  pthread_mutex_t latch;
  row_t* latest_row;
  ts_t latest_wts;
  MvccWriteEntry write_history[MVCC_HIS_LEN];
  bool exists_prewrite;
  ts_t prewrite_ts;
  uint32_t prewrite_his_id;
  ts_t max_served_rts;
};

struct MvccTxnState {
  ts_t ts;
};

// ---- Lifecycle -----------------------------------------------------------

void cc_init_row_state(row_t* row) {
  MvccRowState* s = (MvccRowState*)_mm_malloc(sizeof(MvccRowState), 64);
  pthread_mutex_init(&s->latch, NULL);
  for (int i = 0; i < MVCC_HIS_LEN; i++) {
    s->write_history[i].valid = false;
    s->write_history[i].reserved = false;
    s->write_history[i].row = NULL;
  }
  s->latest_row = row;
  s->latest_wts = 0;
  s->exists_prewrite = false;
  s->max_served_rts = 0;
  row->cc_row_state = s;
}

void cc_free_row_state(row_t* row) {
  MvccRowState* s = (MvccRowState*)row->cc_row_state;
  if (!s) {
    return;
  }
  for (int i = 0; i < MVCC_HIS_LEN; i++) {
    if (s->write_history[i].row) {
      s->write_history[i].row->free_row();
      _mm_free(s->write_history[i].row);
    }
  }
  pthread_mutex_destroy(&s->latch);
  _mm_free(s);
  row->cc_row_state = NULL;
}

void cc_init_txn_man(txn_man*) {}
void cc_global_init() {}

// ---- Transaction setup ---------------------------------------------------

void cc_pre_txn(thread_t* thd, txn_man* txn, base_query* q) {
  (void)q;
  MvccTxnState* ts = (MvccTxnState*)malloc(sizeof(MvccTxnState));
  ts->ts = glob_manager->get_ts(thd->get_thd_id());
  txn->set_ts(ts->ts);
  glob_manager->add_ts(thd->get_thd_id(), ts->ts);
  txn->cc_txn_state = ts;
}

void cc_post_txn(thread_t* thd, txn_man* txn, RC rc) {
  (void)thd;
  (void)rc;
  if (txn->cc_txn_state) {
    free(txn->cc_txn_state);
    txn->cc_txn_state = NULL;
  }
}

// ---- Helper: find a free slot, GC oldest version if necessary -----------
// Must be called with s->latch held.
static int find_or_gc_slot(MvccRowState* s) {
  // First pass: find a free slot.
  for (int i = 0; i < MVCC_HIS_LEN; i++) {
    if (!s->write_history[i].valid && !s->write_history[i].reserved) {
      return i;
    }
  }
  // History full — reclaim the oldest committed version that isn't latest_row.
  ts_t min_ts = UINT64_MAX;
  int victim = -1;
  for (int i = 0; i < MVCC_HIS_LEN; i++) {
    if (s->write_history[i].valid && !s->write_history[i].reserved &&
        s->write_history[i].row != s->latest_row &&
        s->write_history[i].ts < min_ts) {
      min_ts = s->write_history[i].ts;
      victim = i;
    }
  }
  if (victim >= 0) {
    s->write_history[victim].valid = false;
    // Keep the row buffer for reuse; just mark the slot free.
  }
  return victim;
}

// ---- Per-operation hooks -------------------------------------------------

RC cc_pre_op(txn_man* txn, row_t* orig_row, access_t type, int op_idx) {
  (void)op_idx;
  MvccTxnState* ts = (MvccTxnState*)txn->cc_txn_state;
  MvccRowState* s = (MvccRowState*)orig_row->cc_row_state;

  if (type == RD || type == SCAN) {
    return RCOK;  // deferred to cc_post_op
  }

  // type == WR: P_REQ — reserve a write slot.
  pthread_mutex_lock(&s->latch);
  RC rc;
  if (ts->ts < s->latest_wts || ts->ts < s->max_served_rts ||
      (s->exists_prewrite && s->prewrite_ts > ts->ts)) {
    rc = Abort;
  } else if (s->exists_prewrite) {
    pthread_mutex_unlock(&s->latch);
    return Abort;  // simplified: abort instead of WAIT
  } else {
    int idx = find_or_gc_slot(s);
    if (idx < 0) {
      rc = Abort;  // all slots are latest_row or reserved; can't reclaim
    } else {
      if (!s->write_history[idx].row) {
        s->write_history[idx].row = (row_t*)_mm_malloc(sizeof(row_t), 64);
        s->write_history[idx].row->init(MAX_TUPLE_SIZE);
      }
      s->write_history[idx].row->table = s->latest_row->table;
      s->write_history[idx].valid = false;
      s->write_history[idx].reserved = true;
      s->write_history[idx].ts = ts->ts;
      s->exists_prewrite = true;
      s->prewrite_his_id = (uint32_t)idx;
      s->prewrite_ts = ts->ts;
      s->write_history[idx].row->copy(s->latest_row);
      rc = RCOK;
    }
  }
  pthread_mutex_unlock(&s->latch);
  return rc;
}

RC cc_post_op(txn_man* txn, row_t* orig_row, row_t** local_inout,
                access_t type, int op_idx) {
  (void)op_idx;
  MvccTxnState* ts = (MvccTxnState*)txn->cc_txn_state;
  MvccRowState* s = (MvccRowState*)orig_row->cc_row_state;

  if (type == WR) {
    // Point local at the reserved write buffer.
    pthread_mutex_lock(&s->latch);
    row_t* write_buf = s->write_history[s->prewrite_his_id].row;
    pthread_mutex_unlock(&s->latch);
    *local_inout = write_buf;
    txn->accesses[txn->row_cnt]->data = write_buf;
    return RCOK;
  }

  // type == RD / SCAN: find version with largest wts < ts->ts.
  pthread_mutex_lock(&s->latch);
  row_t* chosen = NULL;
  if (ts->ts > s->latest_wts) {
    chosen = s->latest_row;
    if (ts->ts > s->max_served_rts) {
      s->max_served_rts = ts->ts;
    }
  } else {
    ts_t best_ts = 0;
    for (int i = 0; i < MVCC_HIS_LEN; i++) {
      if (s->write_history[i].valid && s->write_history[i].ts < ts->ts &&
          s->write_history[i].ts > best_ts) {
        best_ts = s->write_history[i].ts;
        chosen = s->write_history[i].row;
      }
    }
    if (!chosen) {
      chosen = orig_row;
    }
  }
  pthread_mutex_unlock(&s->latch);

  row_t* copy_row = (row_t*)_mm_malloc(sizeof(row_t), 64);
  copy_row->init(orig_row->get_table(), orig_row->get_part_id());
  copy_row->copy(chosen);
  *local_inout = copy_row;
  txn->accesses[txn->row_cnt]->data = copy_row;
  return RCOK;
}

// ---- Commit validation ---------------------------------------------------

RC cc_pre_commit(txn_man* txn) {
  (void)txn;
  return RCOK;  // MVCC: conflicts caught at P_REQ time.
}

// ---- Cleanup -------------------------------------------------------------

void cc_release_op(txn_man* txn, row_t* orig_row, row_t* local_row,
                   access_t type, int op_idx) {
  (void)op_idx;
  MvccTxnState* ts = (MvccTxnState*)txn->cc_txn_state;
  MvccRowState* s = (MvccRowState*)orig_row->cc_row_state;

  if (type == WR) {
    // Committed write: install version.
    pthread_mutex_lock(&s->latch);
    s->write_history[s->prewrite_his_id].valid = true;
    s->latest_wts = ts->ts;
    s->latest_row = local_row;
    s->exists_prewrite = false;
    pthread_mutex_unlock(&s->latch);
  } else if (type == XP) {
    // Aborted write: release reserved slot.
    pthread_mutex_lock(&s->latch);
    s->write_history[s->prewrite_his_id].valid = false;
    s->write_history[s->prewrite_his_id].reserved = false;
    s->exists_prewrite = false;
    pthread_mutex_unlock(&s->latch);
  } else {
    // RD: free the local copy.
    if (local_row && local_row != orig_row) {
      local_row->free_row();
      _mm_free(local_row);
    }
  }
}
