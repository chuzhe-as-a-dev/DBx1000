#include "manager.h"
#include "row.h"
#include "row_hekaton.h"
#include "txn.h"

static inline Row_hekaton* cc_mgr(row_t* r) {
  return (Row_hekaton*)r->cc_row_state;
}

RC txn_man::validate_hekaton(RC rc) {
  uint64_t starttime = get_sys_clock();
  INC_STATS(get_thd_id(), debug1, get_sys_clock() - starttime);
  ts_t commit_ts = glob_manager->get_ts(get_thd_id());
  // validate the read set.
#if ISOLATION_LEVEL == SERIALIZABLE
  if (rc == RCOK) {
    for (int rid = 0; rid < row_cnt; rid++) {
      if (accesses[rid]->type == WR) {
        continue;
      }
      rc = cc_mgr(accesses[rid]->orig_row)
               ->prepare_read(this, accesses[rid]->data, commit_ts);
      if (rc == Abort) {
        break;
      }
    }
  }
#endif
  // postprocess
  for (int rid = 0; rid < row_cnt; rid++) {
    if (accesses[rid]->type == RD) {
      continue;
    }
    cc_mgr(accesses[rid]->orig_row)->post_process(this, commit_ts, rc);
  }
  return rc;
}
