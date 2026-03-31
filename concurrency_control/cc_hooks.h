#pragma once

// Forward declarations — avoid pulling in full headers here to prevent
// circular include chains (cc_hooks.h is included by txn.h and row.h).
class row_t;
class txn_man;
class thread_t;
class base_query;
#include "global.h"  // RC, access_t, idx_key_t, etc.

// ---- txn_man fields available to hook implementations -------------------
// (Hook .cpp files #include "txn.h" for full access; this documents the
// contract so the generator doesn't need to read txn.h directly.)
//
//   txn->cc_txn_state      void*    — opaque per-txn state; hook owns lifecycle
//   txn->accesses[i]       Access*  — per-access record:
//     ->type                access_t — RD, WR, SCAN
//     ->orig_row            row_t*   — the original row
//     ->data                row_t*   — working copy (set by row_t::get_row,
//                                      updatable by cc_post_op)
//     ->orig_data           row_t*   — pre-image for rollback (hook may use)
//   txn->row_cnt            int      — number of accesses recorded so far
//   txn->wr_cnt             int      — number of write accesses
//   txn->get_thd_id()       uint64_t — thread ID
//   txn->get_ts() / set_ts() ts_t   — transaction timestamp
//
// row_t fields:
//   row->cc_row_state       void*    — opaque per-row state; hook owns lifecycle
//   row->get_data()         char*    — row data buffer
//   row->get_table()        table_t* — table metadata
//   row->get_part_id()      uint64_t — partition ID
//   row->copy(src)          void     — copy data from src row

// ---- Per-row CC state lifecycle ----------------------------------------
// Called from row_t::init_manager() and row free respectively.
// cc_row_state is a void* in row_t that the hook manages entirely.
void cc_init_row_state(row_t* row);
void cc_free_row_state(row_t* row);

// ---- Per-transaction setup/teardown in thread_t::run() -----------------
// Covers: timestamp allocation, partition lock acquisition, ts registration
// with glob_manager.  Called around run_txn().
void cc_pre_txn(thread_t* thd, txn_man* txn, base_query* q);
void cc_post_txn(thread_t* thd, txn_man* txn, RC rc);

// ---- Per-operation hooks -----------------------------------------------
// Called inside txn_man::get_row() under #if CC_ALG == PER_OP.
// cc_pre_op: lock acquisition, timestamp pre-check.  Returns Abort to abort.
// cc_post_op: snapshot/copy decision.  May update *local_inout to a copy.
RC cc_pre_op(txn_man* txn, row_t* orig_row, access_t type, int op_idx);
void cc_post_op(txn_man* txn, row_t* orig_row, row_t** local_inout,
                access_t type, int op_idx);

// ---- Commit-time validation --------------------------------------------
// Called from txn_man::finish() before cleanup for PER_OP.
RC cc_pre_commit(txn_man* txn);

// ---- Per-row release inside row_t::return_row() ------------------------
// Called in reverse acquisition order during cleanup.
// type == WR: committed write — write-back, version install, lock release.
// type == XP: aborted write — rollback, lock release, free any copy.
// type == RD: read release — lock release, free any copy.
void cc_release_op(txn_man* txn, row_t* orig_row, row_t* local_row,
                   access_t type, int op_idx);

// ---- Global CC state initialization ------------------------------------
// Called once from main() after global managers.
void cc_global_init();
