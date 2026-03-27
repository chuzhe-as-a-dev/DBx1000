#include "txn.h"
#include "row.h"
#include "row_occ.h"
#include "mem_alloc.h"

void Row_occ::init(row_t* row) {
  _row = row;
  int part_id = row->get_part_id();
  _latch =
      (pthread_mutex_t*)mem_allocator.alloc(sizeof(pthread_mutex_t), part_id);
  pthread_mutex_init(_latch, NULL);
  wts = 0;
  blatch = false;
}

// Read access under OCC: aborts if the row was written after the txn's
// start_ts, ensuring the txn's snapshot is consistent. On success copies
// the current row data into the txn's private cur_row. (AI-generated)
RC Row_occ::access(txn_man* txn, TsType type) {
  RC rc = RCOK;
  pthread_mutex_lock(_latch);
  if (type == R_REQ) {
    if (txn->start_ts < wts)
      rc = Abort;
    else {
      txn->cur_row->copy(_row);
      rc = RCOK;
    }
  } else
    assert(false);
  pthread_mutex_unlock(_latch);
  return rc;
}

void Row_occ::latch() { pthread_mutex_lock(_latch); }

// Validation check at commit time: the row must not have been written after
// the txn's start_ts, otherwise the read was stale. (AI-generated)
bool Row_occ::validate(uint64_t ts) {
  if (ts < wts)
    return false;
  else
    return true;
}

void Row_occ::write(row_t* data, uint64_t ts) {
  _row->copy(data);
  if (PER_ROW_VALID) {
    assert(ts > wts);
    wts = ts;
  }
}

void Row_occ::release() { pthread_mutex_unlock(_latch); }
