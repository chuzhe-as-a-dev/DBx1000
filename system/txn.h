#pragma once

#include "cc_hooks.h"
#include "global.h"
#include "helper.h"

class workload;
class thread_t;
class row_t;
class table_t;
class base_query;
class INDEX;

// each thread has a txn_man.
// a txn_man corresponds to a single transaction.

// For VLL
enum TxnType { VLL_Blocked, VLL_Free };

// Per-access algorithm-specific fields via template specialization.
template <CCAlg>
struct AccessExtra {};
template <>
struct AccessExtra<CCAlg::Tictoc> {
  ts_t wts;
  ts_t rts;
};
template <>
struct AccessExtra<CCAlg::Silo> {
  ts_t tid;
  ts_t epoch;
};
template <>
struct AccessExtra<CCAlg::Hekaton> {
  void* history_entry;
};

class Access : public AccessExtra<cc_alg> {
 public:
  access_t type;
  row_t* orig_row;
  row_t* data;
  row_t* orig_data;
  void cleanup();
};

// Per-transaction algorithm-specific fields via template specialization.
template <CCAlg>
struct TxnExtra {};
template <>
struct TxnExtra<CCAlg::DlDetect> {
  bool volatile lock_ready = false;
  bool volatile lock_abort = false;
};
template <>
struct TxnExtra<CCAlg::NoWait> {
  bool volatile lock_ready = false;
  bool volatile lock_abort = false;
};
template <>
struct TxnExtra<CCAlg::WaitDie> {
  bool volatile lock_ready = false;
  bool volatile lock_abort = false;
};
template <>
struct TxnExtra<CCAlg::Timestamp> {
  bool volatile ts_ready = false;
};
template <>
struct TxnExtra<CCAlg::Mvcc> {
  bool volatile ts_ready = false;
};
template <>
struct TxnExtra<CCAlg::Hstore> {
  int volatile ready_part = 0;
};
template <>
struct TxnExtra<CCAlg::Occ> {
  uint64_t start_ts;
  uint64_t end_ts;
};
template <>
struct TxnExtra<CCAlg::Hekaton> {
  void* volatile history_entry = nullptr;
};
template <>
struct TxnExtra<CCAlg::Tictoc> {
  ts_t _max_wts;
  bool _atomic_timestamp;
  bool _pre_abort;
  bool _validation_no_wait;
  bool _write_copy_ptr;
  ts_t last_wts;
  ts_t last_rts;
};
template <>
struct TxnExtra<CCAlg::Silo> {
  ts_t _cur_tid;
  bool _pre_abort;
  bool _validation_no_wait;
  ts_t last_tid;
};
template <>
struct TxnExtra<CCAlg::Vll> {
  TxnType vll_txn_type;
};
template <>
struct TxnExtra<CCAlg::PerOp> {
  void* cc_txn_state = nullptr;
};

class txn_man : public TxnExtra<cc_alg> {
 public:
  virtual void init(thread_t* h_thd, workload* h_wl, uint64_t part_id);
  void release();
  thread_t* h_thd;
  workload* h_wl;
  myrand* mrand;
  uint64_t abort_cnt;

  virtual RC run_txn(base_query* m_query) = 0;
  uint64_t get_thd_id();
  workload* get_wl();
  void set_txn_id(txnid_t txn_id);
  txnid_t get_txn_id();

  void set_ts(ts_t timestamp);
  ts_t get_ts();

  pthread_mutex_t txn_lock;
  row_t* volatile cur_row;
  RC finish(RC rc);
  void cleanup(RC rc);
#if CC_ALG == TICTOC
  ts_t get_max_wts() { return _max_wts; }
  void update_max_wts(ts_t max_wts);
#endif

  int row_cnt;
  int wr_cnt;
  Access** accesses;
  int num_accesses_alloc;

  itemid_t* index_read(INDEX* index, idx_key_t key, int part_id);
  void index_read(INDEX* index, idx_key_t key, int part_id, itemid_t*& item);
  row_t* get_row(row_t* row, access_t type, int op_idx = -1);

  // Validation methods — declared unconditionally; defined only in the
  // corresponding CC algorithm's .cpp file.
  RC validate_tictoc();
  RC validate_silo();
  RC validate_hekaton(RC rc);

 protected:
  void insert_row(row_t* row, table_t* table);

 private:
  // insert rows
  uint64_t insert_cnt;
  row_t* insert_rows[MAX_ROW_PER_TXN];
  txnid_t txn_id;
  ts_t timestamp;
};
