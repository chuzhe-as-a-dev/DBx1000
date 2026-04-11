// cc_hooks_no_wait.cpp — NO_WAIT 2PL implementation of the cc_hooks interface.
//
// Per-row state: pthread_rwlock (SH for reads, EX for writes) + version
// counter. cc_pre_op: tryrdlock for RD/SCAN, trywrlock for WR. Abort on
// contention. cc_post_op: noop — workload operates on orig_row under lock.
// cc_pre_commit: noop — all locks still held.
// cc_release_op: increment version on committed write (WR), unlock.

#include <pthread.h>

#include <cstdlib>

#include "cc_hooks.h"
#include "row.h"
#include "txn.h"

struct RowCCState {
  pthread_rwlock_t rwlock;
};

void cc_init_row_state(row_t* row) {
  RowCCState* s = (RowCCState*)malloc(sizeof(RowCCState));
  pthread_rwlock_init(&s->rwlock, NULL);
  row->cc_row_state = s;
}

void cc_free_row_state(row_t* row) {
  RowCCState* s = (RowCCState*)row->cc_row_state;
  if (s) {
    pthread_rwlock_destroy(&s->rwlock);
    free(s);
    row->cc_row_state = NULL;
  }
}

void cc_pre_txn(thread_t* thd, txn_man* txn, base_query* q) {
  (void)thd;
  (void)txn;
  (void)q;
}

void cc_post_txn(thread_t* thd, txn_man* txn, RC rc) {
  (void)thd;
  (void)txn;
  (void)rc;
}

void cc_init_txn_man(txn_man*) {}
void cc_global_init() {}

RC cc_pre_op(txn_man* txn, row_t* orig_row, access_t type, int op_idx) {
  (void)txn;
  (void)op_idx;
  RowCCState* s = (RowCCState*)orig_row->cc_row_state;
  if (type == WR) {
    if (pthread_rwlock_trywrlock(&s->rwlock) != 0) {
      return Abort;
    }
  } else {
    if (pthread_rwlock_tryrdlock(&s->rwlock) != 0) {
      return Abort;
    }
  }
  return RCOK;
}

RC cc_post_op(txn_man* txn, row_t* orig_row, row_t** local_inout,
                access_t type, int op_idx) {
  (void)txn;
  (void)orig_row;
  (void)local_inout;
  (void)type;
  (void)op_idx;
  return RCOK;
}

RC cc_pre_commit(txn_man* txn) {
  (void)txn;
  return RCOK;
}

void cc_release_op(txn_man* txn, row_t* orig_row, row_t* local_row,
                   access_t type, int op_idx) {
  (void)txn;
  (void)local_row;
  (void)op_idx;
  RowCCState* s = (RowCCState*)orig_row->cc_row_state;
  pthread_rwlock_unlock(&s->rwlock);
}
