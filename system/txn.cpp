#include "txn.h"

#include "catalog.h"
#include "cc_hooks.h"
#include "index_btree.h"
#include "index_hash.h"
#include "mem_alloc.h"
#include "occ.h"
#include "row.h"
#include "table.h"
#include "thread.h"
#include "wl.h"
#include "ycsb.h"

// Allocates the per-transaction access array (MAX_ROW_PER_TXN entries,
// NULL-init'd so first access to a slot triggers lazy allocation of the Access
// struct). Also reads runtime parameters specific to TICTOC/SILO:
//   pre_abort        – abort early on obvious conflict before reaching
//   validation. validation_lock  – no-wait vs. blocking during the validation
//   phase. write_copy_form  – TICTOC: store write-set as pointer vs. data copy.
//   atomic_timestamp – TICTOC: use single atomic word for wts+rts pair.
//   (AI-generated)
void txn_man::init(thread_t* h_thd, workload* h_wl, uint64_t thd_id) {
  this->h_thd = h_thd;
  this->h_wl = h_wl;
  pthread_mutex_init(&txn_lock, NULL);
  row_cnt = 0;
  wr_cnt = 0;
  insert_cnt = 0;
  accesses = (Access**)_mm_malloc(sizeof(Access*) * MAX_ROW_PER_TXN, 64);
  for (int i = 0; i < MAX_ROW_PER_TXN; i++) accesses[i] = NULL;
  num_accesses_alloc = 0;
  // Initialize CC-specific fields from TxnExtra<cc_alg>.
  [this]<CCAlg A = cc_alg>() {
    auto& extra = static_cast<TxnExtra<A>&>(*this);
    if constexpr (A == CCAlg::Tictoc || A == CCAlg::Silo) {
      extra._pre_abort = (g_params["pre_abort"] == "true");
      if (g_params["validation_lock"] == "no-wait")
        extra._validation_no_wait = true;
      else if (g_params["validation_lock"] == "waiting")
        extra._validation_no_wait = false;
      else
        assert(false);
    }
    if constexpr (A == CCAlg::Tictoc) {
      extra._max_wts = 0;
      extra._write_copy_ptr = (g_params["write_copy_form"] == "ptr");
      extra._atomic_timestamp = (g_params["atomic_timestamp"] == "true");
    } else if constexpr (A == CCAlg::Silo) {
      extra._cur_tid = 0;
    }
  }();
}

void txn_man::set_txn_id(txnid_t txn_id) { this->txn_id = txn_id; }

txnid_t txn_man::get_txn_id() { return this->txn_id; }

workload* txn_man::get_wl() { return h_wl; }

uint64_t txn_man::get_thd_id() { return h_thd->get_thd_id(); }

void txn_man::set_ts(ts_t timestamp) { this->timestamp = timestamp; }

ts_t txn_man::get_ts() { return this->timestamp; }

// Returns all accessed rows to their CC managers and resets per-txn counters.
// Rows are released in reverse acquisition order to avoid deadlock on cleanup.
// On abort, write accesses become XP (expire) so the manager can restore the
// pre-image if ROLL_BACK is enabled; the original data copy made in get_row()
// is passed for algorithms that need it (DL_DETECT / NO_WAIT / WAIT_DIE).
// Any rows inserted by this transaction are freed on abort.
// HEKATON skips all of this because it manages its own version chain.
// (AI-generated)
void txn_man::cleanup(RC rc) {
  if constexpr (cc_alg == CCAlg::Hekaton) {
    row_cnt = 0;
    wr_cnt = 0;
    insert_cnt = 0;
    return;
  }
  for (int rid = row_cnt - 1; rid >= 0; rid--) {
    row_t* orig_r = accesses[rid]->orig_row;
    access_t type = accesses[rid]->type;
    if (type == WR && rc == Abort) type = XP;

    if constexpr ((cc_alg == CCAlg::NoWait || cc_alg == CCAlg::DlDetect) &&
                  iso_level == IsoLevel::RepeatableRead) {
      if (type == RD) {
        accesses[rid]->data = NULL;
        continue;
      }
    }

    [&]<CCAlg A = cc_alg>() {
      if constexpr (A == CCAlg::DlDetect || A == CCAlg::NoWait ||
                    A == CCAlg::WaitDie) {
        if (roll_back && type == XP) {
          auto& acc = static_cast<AccessExtra<A>&>(*accesses[rid]);
          orig_r->return_row(type, this, acc.orig_data, rid);
        } else {
          orig_r->return_row(type, this, accesses[rid]->data, rid);
        }
      } else {
        orig_r->return_row(type, this, accesses[rid]->data, rid);
      }
    }();

    if constexpr (cc_alg != CCAlg::Tictoc && cc_alg != CCAlg::Silo) {
      accesses[rid]->data = NULL;
    }
  }

  if (rc == Abort) {
    for (UInt32 i = 0; i < insert_cnt; i++) {
      row_t* row = insert_rows[i];
      assert(g_part_alloc == false);
      if constexpr (cc_alg != CCAlg::Hstore && cc_alg != CCAlg::Occ &&
                    cc_alg != CCAlg::PerOp) {
        mem_allocator.free(row->cc_row_state, 0);
      }
      row->free_row();
      mem_allocator.free(row, sizeof(row_t));
    }
  }
  row_cnt = 0;
  wr_cnt = 0;
  insert_cnt = 0;
  if constexpr (cc_alg == CCAlg::DlDetect) {
    dl_detector.clear_dep(get_txn_id());
  }
}

// Acquires access to a row under the current CC algorithm and records the
// access in the per-txn access log for cleanup/validation later.
//
// Copy semantics by algorithm:
//   SILO/TICTOC – allocate both data (working copy) and orig_data; also save
//                 the observed wts/rts (TICTOC) or TID (SILO) for validation.
//   DL_DETECT/NO_WAIT/WAIT_DIE – allocate orig_data for rollback; on write,
//                 snapshot the current row so cleanup can restore it.
//   Others – no extra copy; data points directly to the row buffer.
//
// Under REPEATABLE_READ for NO_WAIT/DL_DETECT, read locks are released
// immediately after acquisition (lock-then-release), so the row manager is
// notified here rather than in cleanup(). (AI-generated)
row_t* txn_man::get_row(row_t* row, access_t type, int op_idx) {
  if constexpr (cc_alg == CCAlg::Hstore) return row;
  uint64_t starttime = get_sys_clock();
  RC rc = RCOK;
  if (accesses[row_cnt] == NULL) {
    Access* access = (Access*)_mm_malloc(sizeof(Access), 64);
    accesses[row_cnt] = access;
    if constexpr (cc_alg == CCAlg::Silo || cc_alg == CCAlg::Tictoc) {
      access->data = (row_t*)_mm_malloc(sizeof(row_t), 64);
      access->data->init(MAX_TUPLE_SIZE);
    }
    [&]<CCAlg A = cc_alg>() {
      if constexpr (A == CCAlg::DlDetect || A == CCAlg::NoWait ||
                    A == CCAlg::WaitDie) {
        auto& acc = static_cast<AccessExtra<A>&>(*access);
        acc.orig_data = (row_t*)_mm_malloc(sizeof(row_t), 64);
        acc.orig_data->init(MAX_TUPLE_SIZE);
      }
    }();
    num_accesses_alloc++;
  }

  rc = row->get_row(type, this, accesses[row_cnt]->data, op_idx);

  if (rc == Abort) {
    return NULL;
  }
  accesses[row_cnt]->type = type;
  accesses[row_cnt]->orig_row = row;
  [this]<CCAlg A = cc_alg>() {
    if constexpr (A == CCAlg::Tictoc) {
      auto& acc = static_cast<AccessExtra<A>&>(*accesses[row_cnt]);
      auto& txn = static_cast<TxnExtra<A>&>(*this);
      acc.wts = txn.last_wts;
      acc.rts = txn.last_rts;
    } else if constexpr (A == CCAlg::Silo) {
      auto& acc = static_cast<AccessExtra<A>&>(*accesses[row_cnt]);
      auto& txn = static_cast<TxnExtra<A>&>(*this);
      acc.tid = txn.last_tid;
    } else if constexpr (A == CCAlg::Hekaton) {
      auto& acc = static_cast<AccessExtra<A>&>(*accesses[row_cnt]);
      auto& txn = static_cast<TxnExtra<A>&>(*this);
      acc.history_entry = txn.history_entry;
    }
  }();

  [this, row, type]<CCAlg A = cc_alg>() {
    if constexpr (A == CCAlg::DlDetect || A == CCAlg::NoWait ||
                  A == CCAlg::WaitDie) {
      if (ROLL_BACK && type == WR) {
        auto& acc = static_cast<AccessExtra<A>&>(*accesses[row_cnt]);
        acc.orig_data->table = row->get_table();
        acc.orig_data->copy(row);
      }
    }
  }();

  if constexpr ((cc_alg == CCAlg::NoWait || cc_alg == CCAlg::DlDetect) &&
                iso_level == IsoLevel::RepeatableRead) {
    if (type == RD) row->return_row(type, this, accesses[row_cnt]->data);
  }

  if constexpr (cc_alg == CCAlg::PerOp) {
    cc_post_op(this, row, &accesses[row_cnt]->data, type, op_idx);
  }

  row_cnt++;
  if (type == WR) wr_cnt++;

  uint64_t timespan = get_sys_clock() - starttime;
  INC_TMP_STATS(get_thd_id(), time_man, timespan);
  return accesses[row_cnt - 1]->data;
}

void txn_man::insert_row(row_t* row, table_t* table) {
  if constexpr (cc_alg == CCAlg::Hstore) return;
  assert(insert_cnt < MAX_ROW_PER_TXN);
  insert_rows[insert_cnt++] = row;
}

itemid_t* txn_man::index_read(INDEX* index, idx_key_t key, int part_id) {
  uint64_t starttime = get_sys_clock();
  itemid_t* item;
  index->index_read(key, item, part_id, get_thd_id());
  INC_TMP_STATS(get_thd_id(), time_index, get_sys_clock() - starttime);
  return item;
}

void txn_man::index_read(INDEX* index, idx_key_t key, int part_id,
                         itemid_t*& item) {
  uint64_t starttime = get_sys_clock();
  index->index_read(key, item, part_id, get_thd_id());
  INC_TMP_STATS(get_thd_id(), time_index, get_sys_clock() - starttime);
}

RC txn_man::finish(RC rc) {
  return [&]<CCAlg A = cc_alg>() -> RC {
    if constexpr (A == CCAlg::Hstore) {
      return RCOK;
    } else {
      uint64_t starttime = get_sys_clock();
      if constexpr (A == CCAlg::Occ) {
        if (rc == RCOK)
          rc = occ_man.validate(this);
        else
          cleanup(rc);
      } else if constexpr (A == CCAlg::Tictoc) {
        if (rc == RCOK)
          rc = validate_tictoc();
        else
          cleanup(rc);
      } else if constexpr (A == CCAlg::Silo) {
        if (rc == RCOK)
          rc = validate_silo();
        else
          cleanup(rc);
      } else if constexpr (A == CCAlg::Hekaton) {
        rc = validate_hekaton(rc);
        cleanup(rc);
      } else if constexpr (A == CCAlg::PerOp) {
        if (rc == RCOK) rc = cc_pre_commit(this);
        cleanup(rc);
      } else {
        cleanup(rc);
      }
      uint64_t timespan = get_sys_clock() - starttime;
      INC_TMP_STATS(get_thd_id(), time_man, timespan);
      INC_STATS(get_thd_id(), time_cleanup, timespan);
      return rc;
    }
  }();
}

void txn_man::release() {
  for (int i = 0; i < num_accesses_alloc; i++)
    mem_allocator.free(accesses[i], 0);
  mem_allocator.free(accesses, 0);
}
