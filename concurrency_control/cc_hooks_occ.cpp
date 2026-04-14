// cc_hooks_occ.cpp — OCC (per-row validation) implementation of cc_hooks.
//
// Per-row state: pthread_mutex (latch) + write timestamp (wts).
// cc_pre_op: early-abort check (start_ts < row.wts → stale).
// cc_post_op: allocate private copy, record observed wts.
// cc_pre_commit: sort accesses by key, latch all, re-validate,
//                install writes, release latches.
// cc_release_op: free local copy.

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

// ---- Per-row CC state ----------------------------------------------------
struct OccRowState {
  pthread_mutex_t latch;
  ts_t wts;  // last committed write timestamp
};

// ---- Per-transaction CC state --------------------------------------------
struct OccTxnState {
  ts_t start_ts;
  ts_t end_ts;
  ts_t observed_wts[MAX_ROW_PER_TXN];
  int lock_cnt;
};

// ---- Lifecycle -----------------------------------------------------------

void cc_init_row_state(row_t* row) {
  OccRowState* s = (OccRowState*)malloc(sizeof(OccRowState));
  pthread_mutex_init(&s->latch, NULL);
  s->wts = 0;
  row->cc_row_state = s;
}

void cc_free_row_state(row_t* row) {
  OccRowState* s = (OccRowState*)row->cc_row_state;
  if (s) {
    pthread_mutex_destroy(&s->latch);
    free(s);
    row->cc_row_state = NULL;
  }
}

void cc_init_txn_man(txn_man*) {}
void cc_global_init() {}

// ---- Transaction setup ---------------------------------------------------

void cc_pre_txn(thread_t* thd, txn_man* txn, base_query* q) {
  (void)q;
  OccTxnState* ts = (OccTxnState*)malloc(sizeof(OccTxnState));
  ts->start_ts = glob_manager->get_ts(thd->get_thd_id());
  ts->end_ts = 0;
  ts->lock_cnt = 0;
  memset(ts->observed_wts, 0, sizeof(ts->observed_wts));
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

// ---- Per-operation hooks -------------------------------------------------

RC cc_pre_op(txn_man* txn, row_t* orig_row, access_t type, int op_idx) {
  (void)type;
  (void)op_idx;
  OccRowState* s = (OccRowState*)orig_row->cc_row_state;
  OccTxnState* ts = (OccTxnState*)txn->cc_txn_state;

  // Early-abort: if row was written after our start_ts, we're stale.
  pthread_mutex_lock(&s->latch);
  bool stale = (ts->start_ts < s->wts);
  pthread_mutex_unlock(&s->latch);

  if (stale) {
    return Abort;
  }
  return RCOK;
}

// cc_post_op is called in txn_man::get_row() after access recording.
// txn->accesses[txn->row_cnt] is the current access (row_cnt not yet
// incremented).
RC cc_post_op(txn_man* txn, row_t* orig_row, row_t** local_inout, access_t type,
              int op_idx) {
  (void)type;
  OccRowState* s = (OccRowState*)orig_row->cc_row_state;
  OccTxnState* ts = (OccTxnState*)txn->cc_txn_state;

  // Allocate a private copy.
  row_t* copy_row = (row_t*)_mm_malloc(sizeof(row_t), 64);
  copy_row->init(orig_row->get_table(), orig_row->get_part_id());
  copy_row->copy(orig_row);

  // Record the wts we observed (for validation).
  pthread_mutex_lock(&s->latch);
  ts->observed_wts[op_idx] = s->wts;
  pthread_mutex_unlock(&s->latch);

  // Point the workload at our copy.
  *local_inout = copy_row;
  // Also update accesses[]->data (row_cnt not yet incremented).
  txn->accesses[txn->row_cnt]->data = copy_row;
  return RCOK;
}

// ---- Commit validation ---------------------------------------------------

RC cc_pre_commit(txn_man* txn) {
  OccTxnState* ts = (OccTxnState*)txn->cc_txn_state;

  // Sort accesses by (table_name, primary_key) to avoid deadlock.
  for (int i = txn->row_cnt - 1; i > 0; i--) {
    for (int j = 0; j < i; j++) {
      int tabcmp = strcmp(txn->accesses[j]->orig_row->get_table_name(),
                          txn->accesses[j + 1]->orig_row->get_table_name());
      if (tabcmp > 0 ||
          (tabcmp == 0 &&
           txn->accesses[j]->orig_row->get_primary_key() >
               txn->accesses[j + 1]->orig_row->get_primary_key())) {
        Access* tmp = txn->accesses[j];
        txn->accesses[j] = txn->accesses[j + 1];
        txn->accesses[j + 1] = tmp;
        ts_t wts_tmp = ts->observed_wts[j];
        ts->observed_wts[j] = ts->observed_wts[j + 1];
        ts->observed_wts[j + 1] = wts_tmp;
      }
    }
  }

  // Latch all rows and validate.
  bool ok = true;
  ts->lock_cnt = 0;
  for (int i = 0; i < txn->row_cnt && ok; i++) {
    OccRowState* s = (OccRowState*)txn->accesses[i]->orig_row->cc_row_state;
    pthread_mutex_lock(&s->latch);
    ts->lock_cnt++;
    if (ts->start_ts < s->wts) {
      ok = false;
    }
  }

  if (ok) {
    // Advance global timestamp to get end_ts.
    ts->end_ts = glob_manager->get_ts(txn->get_thd_id());

    // Install writes.
    for (int i = 0; i < txn->row_cnt; i++) {
      if (txn->accesses[i]->type == WR) {
        row_t* orig = txn->accesses[i]->orig_row;
        OccRowState* s = (OccRowState*)orig->cc_row_state;
        orig->copy(txn->accesses[i]->data);
        s->wts = ts->end_ts;
      }
    }
  }

  // Release all latches.
  for (int i = 0; i < ts->lock_cnt; i++) {
    OccRowState* s = (OccRowState*)txn->accesses[i]->orig_row->cc_row_state;
    pthread_mutex_unlock(&s->latch);
  }

  return ok ? RCOK : Abort;
}

// ---- Cleanup -------------------------------------------------------------

void cc_release_op(txn_man* txn, row_t* orig_row, row_t* local_row,
                   access_t type, int op_idx) {
  (void)txn;
  (void)orig_row;
  (void)type;
  (void)op_idx;
  // Free the local copy (writes already installed in cc_pre_commit).
  if (local_row && local_row != orig_row) {
    local_row->free_row();
    _mm_free(local_row);
  }
}
