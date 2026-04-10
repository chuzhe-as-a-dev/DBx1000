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

// Forward declarations — the actual class definitions are in row_*.h headers,
// but those are wrapped in #if CC_ALG == X guards and may be empty for
// non-matching algorithms. The forward declarations allow RowManagerType
// specializations to name the types; only the matching specialization is ever
// instantiated (via template lambda).
class Row_lock;
class Row_ts;
class Row_mvcc;
class Row_hekaton;
class Row_occ;
class Row_tictoc;
class Row_silo;
class Row_vll;

// Maps CCAlg → the concrete Row_* manager type for baseline algorithms.
// Used inside template lambdas to cast cc_row_state to the correct type.
template <CCAlg A>
struct RowManagerType;
#define MGR(alg, cls)                 \
  template <>                         \
  struct RowManagerType<CCAlg::alg> { \
    using type = cls;                 \
  }
MGR(DlDetect, Row_lock);
MGR(NoWait, Row_lock);
MGR(WaitDie, Row_lock);
MGR(Timestamp, Row_ts);
MGR(Mvcc, Row_mvcc);
MGR(Hekaton, Row_hekaton);
MGR(Occ, Row_occ);
MGR(Tictoc, Row_tictoc);
MGR(Silo, Row_silo);
MGR(Vll, Row_vll);
MGR(Hstore, void);
#undef MGR

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
// PER_OP manages its own row state via cc_hooks.
// (AI-generated)
void row_t::init_manager(row_t* row) {
  [&]<CCAlg A = cc_alg>() {
    if constexpr (A == CCAlg::PerOp) {
      cc_init_row_state(this);
      (void)row;
      return;
    } else if constexpr (A == CCAlg::Hstore) {
      // HSTORE has no per-row manager.
      return;
    } else {
      using Mgr = typename RowManagerType<A>::type;
      if constexpr (A == CCAlg::DlDetect || A == CCAlg::NoWait ||
                    A == CCAlg::WaitDie || A == CCAlg::Timestamp ||
                    A == CCAlg::Occ || A == CCAlg::Vll) {
        cc_row_state = (Mgr*)mem_allocator.alloc(sizeof(Mgr), _part_id);
      } else {
        cc_row_state = (Mgr*)_mm_malloc(sizeof(Mgr), 64);
      }
      reinterpret_cast<Mgr*>(cc_row_state)->init(this);
    }
  }();
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
// HSTORE / VLL: no per-row locking; return the row directly.
// PER_OP: delegates to cc_pre_op() hook. (AI-generated)
RC row_t::get_row(access_t type, txn_man* txn, row_t*& row, int op_idx) {
  return [&]<CCAlg A = cc_alg>() -> RC {
    RC rc = RCOK;
    if constexpr (A == CCAlg::PerOp) {
      rc = cc_pre_op(txn, this, type, op_idx);
      if (rc != RCOK) {
        return rc;
      }
      row = this;
      return RCOK;
    } else if constexpr (A == CCAlg::WaitDie || A == CCAlg::NoWait ||
                         A == CCAlg::DlDetect) {
      using Mgr = typename RowManagerType<A>::type;
      auto* mgr = reinterpret_cast<Mgr*>(this->cc_row_state);
      uint64_t thd_id = txn->get_thd_id();
      lock_t lt = (type == RD || type == SCAN) ? LOCK_SH : LOCK_EX;
      [[maybe_unused]] uint64_t* txnids = nullptr;
      [[maybe_unused]] int txncnt = 0;
      if constexpr (A == CCAlg::DlDetect) {
        rc = mgr->lock_get(lt, txn, txnids, txncnt);
      } else {
        rc = mgr->lock_get(lt, txn);
      }

      if (rc == RCOK) {
        row = this;
      } else if (rc == Abort) {
      } else if (rc == WAIT) {
        ASSERT(A == CCAlg::WaitDie || A == CCAlg::DlDetect);
        auto& te = static_cast<TxnExtra<A>&>(*txn);
        uint64_t starttime = get_sys_clock();
        [[maybe_unused]] bool dep_added = false;
        uint64_t endtime;
        te.lock_abort = false;
        INC_STATS(txn->get_thd_id(), wait_cnt, 1);
        while (!te.lock_ready && !te.lock_abort) {
          if constexpr (A == CCAlg::WaitDie) {
            continue;
          } else if constexpr (A == CCAlg::DlDetect) {
            uint64_t last_detect = starttime;
            uint64_t last_try = starttime;

            uint64_t now = get_sys_clock();
            if (now - starttime > g_timeout) {
              te.lock_abort = true;
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
                if (ok == 0) {
                  dep_added = true;
                } else if (ok == 16) {
                  last_try = now;
                }
              }
              if (dep_added) {
                ok = dl_detector.detect_cycle(txn->get_txn_id());
                if (ok == 16) {  // failed to lock the deadlock detector
                  last_try = now;
                } else if (ok == 0) {
                  last_detect = now;
                } else if (ok == 1) {
                  last_detect = now;
                }
              }
            } else {
              PAUSE
            }
          }
        }
        if (te.lock_ready) {
          rc = RCOK;
        } else if (te.lock_abort) {
          rc = Abort;
          return_row(type, txn, NULL);
        }
        endtime = get_sys_clock();
        INC_TMP_STATS(thd_id, time_wait, endtime - starttime);
        row = this;
      }
      return rc;
    } else if constexpr (A == CCAlg::Timestamp || A == CCAlg::Mvcc ||
                         A == CCAlg::Hekaton) {
      using Mgr = typename RowManagerType<A>::type;
      auto* mgr = reinterpret_cast<Mgr*>(this->cc_row_state);
      uint64_t thd_id = txn->get_thd_id();
      // For TIMESTAMP RD, a new copy of the row will be returned.
      // for MVCC RD, the version will be returned instead of a copy
      // So for MVCC RD-WR, the version should be explicitly copied.
      if constexpr (A == CCAlg::Timestamp) {
        // TODO. should not call malloc for each row read. Only need to call
        // malloc once before simulation starts, like TicToc and Silo.
        txn->cur_row =
            (row_t*)mem_allocator.alloc(sizeof(row_t), this->get_part_id());
        txn->cur_row->init(get_table(), this->get_part_id());
      }

      // TODO need to initialize the table/catalog information.
      TsType ts_type = (type == RD) ? R_REQ : P_REQ;
      rc = mgr->access(txn, ts_type, row);
      if (rc == RCOK) {
        row = txn->cur_row;
      } else if constexpr (A == CCAlg::Timestamp || A == CCAlg::Mvcc) {
        if (rc == WAIT) {
          auto& te = static_cast<TxnExtra<A>&>(*txn);
          uint64_t t1 = get_sys_clock();
          while (!te.ts_ready) {
            PAUSE
          }
          uint64_t t2 = get_sys_clock();
          INC_TMP_STATS(thd_id, time_wait, t2 - t1);
          row = txn->cur_row;
        }
      }
      if (rc != Abort) {
        row->table = get_table();
        assert(row->get_schema() == this->get_schema());
      }
      return rc;
    } else if constexpr (A == CCAlg::Occ) {
      using Mgr = typename RowManagerType<A>::type;
      auto* mgr = reinterpret_cast<Mgr*>(this->cc_row_state);
      // OCC always make a local copy regardless of read or write
      txn->cur_row = (row_t*)mem_allocator.alloc(sizeof(row_t), get_part_id());
      txn->cur_row->init(get_table(), get_part_id());
      rc = mgr->access(txn, R_REQ);
      row = txn->cur_row;
      return rc;
    } else if constexpr (A == CCAlg::Tictoc || A == CCAlg::Silo) {
      using Mgr = typename RowManagerType<A>::type;
      auto* mgr = reinterpret_cast<Mgr*>(this->cc_row_state);
      // like OCC, tictoc also makes a local copy for each read/write
      row->table = get_table();
      TsType ts_type = (type == RD) ? R_REQ : P_REQ;
      rc = mgr->access(txn, ts_type, row);
      return rc;
    } else if constexpr (A == CCAlg::Hstore || A == CCAlg::Vll) {
      row = this;
      return rc;
    } else {
      assert(false);
      return Abort;
    }
  }();
}

// the "row" is the row read out in get_row().
// For locking based CC_ALG, the "row" is the same as "this".
// For timestamp based CC_ALG, the "row" != "this", and the "row" must be freed.
// For MVCC, the row will simply serve as a version. The version will be
// delete during history cleanup.
// For TIMESTAMP, the row will be explicity deleted at the end of access().
// (cf. row_ts.cpp)
void row_t::return_row(access_t type, txn_man* txn, row_t* row, int op_idx) {
  [&]<CCAlg A = cc_alg>() {
    if constexpr (A == CCAlg::PerOp) {
      cc_release_op(txn, this, row, type, op_idx);
      return;
    } else if constexpr (A == CCAlg::WaitDie || A == CCAlg::NoWait ||
                         A == CCAlg::DlDetect) {
      using Mgr = typename RowManagerType<A>::type;
      auto* mgr = reinterpret_cast<Mgr*>(this->cc_row_state);
      assert(row == NULL || row == this || type == XP);
      if (roll_back && type == XP) {  // recover from previous writes.
        this->copy(row);
      }
      mgr->lock_release(txn);
    } else if constexpr (A == CCAlg::Timestamp || A == CCAlg::Mvcc) {
      using Mgr = typename RowManagerType<A>::type;
      auto* mgr = reinterpret_cast<Mgr*>(this->cc_row_state);
      // for RD or SCAN or XP, the row should be deleted.
      // because all WR should be companied by a RD
      // for MVCC RD, the row is not copied, so no need to free.
      if constexpr (A == CCAlg::Timestamp) {
        if (type == RD || type == SCAN) {
          row->free_row();
          mem_allocator.free(row, sizeof(row_t));
        }
      }
      if (type == XP) {
        mgr->access(txn, XP_REQ, row);
      } else if (type == WR) {
        assert(type == WR && row != NULL);
        assert(row->get_schema() == this->get_schema());
        RC rc = mgr->access(txn, W_REQ, row);
        assert(rc == RCOK);
        (void)rc;
      }
    } else if constexpr (A == CCAlg::Occ) {
      using Mgr = typename RowManagerType<A>::type;
      auto* mgr = reinterpret_cast<Mgr*>(this->cc_row_state);
      assert(row != NULL);
      if (type == WR) {
        mgr->write(row, static_cast<TxnExtra<A>&>(*txn).end_ts);
      }
      row->free_row();
      mem_allocator.free(row, sizeof(row_t));
      return;
    } else if constexpr (A == CCAlg::Tictoc || A == CCAlg::Silo) {
      assert(row != NULL);
      return;
    } else if constexpr (A == CCAlg::Hstore || A == CCAlg::Vll) {
      return;
    } else {
      assert(false);
    }
  }();
}
