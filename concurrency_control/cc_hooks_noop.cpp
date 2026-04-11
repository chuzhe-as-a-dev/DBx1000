// cc_hooks_noop.cpp — No-op implementation of the cc_hooks interface.
//
// All hooks are empty. No locking, no copies, no validation.
// Useful as a performance upper bound (zero CC overhead) for
// conflict-free workloads or single-threaded runs.

#include "cc_hooks.h"
#include "row.h"
#include "txn.h"

void cc_init_row_state(row_t* row) { (void)row; }
void cc_free_row_state(row_t* row) { (void)row; }

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
  (void)orig_row;
  (void)type;
  (void)op_idx;
  return RCOK;
}

void cc_post_op(txn_man* txn, row_t* orig_row, row_t** local_inout,
                access_t type, int op_idx) {
  (void)txn;
  (void)orig_row;
  (void)local_inout;
  (void)type;
  (void)op_idx;
}

RC cc_pre_commit(txn_man* txn) {
  (void)txn;
  return RCOK;
}

void cc_release_op(txn_man* txn, row_t* orig_row, row_t* local_row,
                   access_t type, int op_idx) {
  (void)txn;
  (void)orig_row;
  (void)local_row;
  (void)type;
  (void)op_idx;
}
