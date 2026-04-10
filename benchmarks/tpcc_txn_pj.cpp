// tpcc_txn_pj.cpp — TPCC transactions with Polyjuice-compatible op ordering.
//
// Differences from tpcc_txn.cpp (standard DBx1000 TPCC):
//   new_order:
//     - Customer read moved to end (Polyjuice acc_id 10).
//     - Item reads and stock writes run in SEPARATE loops instead of
//       interleaved.  Polyjuice's learned policy was trained against this
//       loop structure; interleaving causes step regressions in the wait
//       mechanism.
//   payment:
//     - No changes (warehouse→district→customer already matches Polyjuice).

#include "index_btree.h"
#include "index_hash.h"
#include "query.h"
#include "row.h"
#include "table.h"
#include "thread.h"
#include "tpcc.h"
#include "tpcc_const.h"
#include "tpcc_helper.h"
#include "tpcc_query.h"
#include "wl.h"

void tpcc_txn_man::init(thread_t* h_thd, workload* h_wl, uint64_t thd_id) {
  txn_man::init(h_thd, h_wl, thd_id);
  _wl = (tpcc_wl*)h_wl;
}

RC tpcc_txn_man::run_txn(base_query* query) {
  tpcc_query* m_query = (tpcc_query*)query;
  switch (m_query->type) {
    case TPCC_PAYMENT:
      return run_payment(m_query);
    case TPCC_NEW_ORDER:
      return run_new_order(m_query);
    default:
      assert(false);
      exit(-1);
  }
}

// Payment: identical to dbx1000's original version.
RC tpcc_txn_man::run_payment(tpcc_query* query) {
  RC rc = RCOK;
  uint64_t key;
  itemid_t* item;
  int op_cnt = 0;

  uint64_t w_id = query->w_id;
  uint64_t c_w_id = query->c_w_id;

  // op 0: warehouse
  key = query->w_id;
  INDEX* index = _wl->i_warehouse;
  item = index_read(index, key, wh_to_part(w_id));
  assert(item != NULL);
  row_t* r_wh = ((row_t*)item->location);
  row_t* r_wh_local;
  if (g_wh_update) {
    r_wh_local = get_row(r_wh, WR, op_cnt++);
  } else {
    r_wh_local = get_row(r_wh, RD, op_cnt++);
  }

  if (r_wh_local == NULL) {
    return finish(Abort);
  }
  double w_ytd;

  r_wh_local->get_value(W_YTD, w_ytd);
  if (g_wh_update) {
    r_wh_local->set_value(W_YTD, w_ytd + query->h_amount);
  }
  char w_name[11];
  char* tmp_str = r_wh_local->get_value(W_NAME);
  memcpy(w_name, tmp_str, 10);
  w_name[10] = '\0';

  // op 1: district
  key = distKey(query->d_id, query->d_w_id);
  item = index_read(_wl->i_district, key, wh_to_part(w_id));
  assert(item != NULL);
  row_t* r_dist = ((row_t*)item->location);
  row_t* r_dist_local = get_row(r_dist, WR, op_cnt++);
  if (r_dist_local == NULL) {
    return finish(Abort);
  }

  double d_ytd;
  r_dist_local->get_value(D_YTD, d_ytd);
  r_dist_local->set_value(D_YTD, d_ytd + query->h_amount);
  char d_name[11];
  tmp_str = r_dist_local->get_value(D_NAME);
  memcpy(d_name, tmp_str, 10);
  d_name[10] = '\0';

  // Customer lookup (by last name or by ID)
  row_t* r_cust;
  if (query->by_last_name) {
    uint64_t key = custNPKey(query->c_last, query->c_d_id, query->c_w_id);
    INDEX* index = _wl->i_customer_last;
    item = index_read(index, key, wh_to_part(c_w_id));
    assert(item != NULL);

    int cnt = 0;
    itemid_t* it = item;
    itemid_t* mid = item;
    while (it != NULL) {
      cnt++;
      it = it->next;
      if (cnt % 2 == 0) {
        mid = mid->next;
      }
    }
    r_cust = ((row_t*)mid->location);
  } else {
    key = custKey(query->c_id, query->c_d_id, query->c_w_id);
    INDEX* index = _wl->i_customer_id;
    item = index_read(index, key, wh_to_part(c_w_id));
    assert(item != NULL);
    r_cust = (row_t*)item->location;
  }

  // op 2: customer
  row_t* r_cust_local = get_row(r_cust, WR, op_cnt++);
  if (r_cust_local == NULL) {
    return finish(Abort);
  }
  double c_balance;
  double c_ytd_payment;
  double c_payment_cnt;

  r_cust_local->get_value(C_BALANCE, c_balance);
  r_cust_local->set_value(C_BALANCE, c_balance - query->h_amount);
  r_cust_local->get_value(C_YTD_PAYMENT, c_ytd_payment);
  r_cust_local->set_value(C_YTD_PAYMENT, c_ytd_payment + query->h_amount);
  r_cust_local->get_value(C_PAYMENT_CNT, c_payment_cnt);
  r_cust_local->set_value(C_PAYMENT_CNT, c_payment_cnt + 1);

  // History insert — skipped (commented out in DBx1000).

  assert(rc == RCOK);
  return finish(rc);
}

// New order: Polyjuice-compatible op ordering.
// Op sequence:
//   op 0:            warehouse RD     (Polyjuice acc_id 0)
//   op 1:            district WR      (acc_ids 1+2 consolidated)
//   op 2..2+N-1:     item[i] RD       (acc_id 3, separate loop)
//   op 2+N..2+2N-1:  stock[i] WR      (acc_ids 4+5 consolidated, separate loop)
//   op 2+2N:         customer RD      (acc_id 10, moved to end)
RC tpcc_txn_man::run_new_order(tpcc_query* query) {
  RC rc = RCOK;
  uint64_t key;
  itemid_t* item;
  INDEX* index;
  int op_cnt = 0;

  bool remote = query->remote;
  uint64_t w_id = query->w_id;
  uint64_t d_id = query->d_id;
  uint64_t c_id = query->c_id;
  uint64_t ol_cnt = query->ol_cnt;

  // op 0: warehouse RD
  key = w_id;
  index = _wl->i_warehouse;
  item = index_read(index, key, wh_to_part(w_id));
  assert(item != NULL);
  row_t* r_wh = ((row_t*)item->location);
  row_t* r_wh_local = get_row(r_wh, RD, op_cnt++);
  if (r_wh_local == NULL) {
    return finish(Abort);
  }

  double w_tax;
  r_wh_local->get_value(W_TAX, w_tax);

  // op 1: district WR (read d_tax + increment d_next_o_id)
  key = distKey(d_id, w_id);
  item = index_read(_wl->i_district, key, wh_to_part(w_id));
  assert(item != NULL);
  row_t* r_dist = ((row_t*)item->location);
  row_t* r_dist_local = get_row(r_dist, WR, op_cnt++);
  if (r_dist_local == NULL) {
    return finish(Abort);
  }
  int64_t o_id;
  o_id = *(int64_t*)r_dist_local->get_value(D_NEXT_O_ID);
  o_id++;
  r_dist_local->set_value(D_NEXT_O_ID, o_id);

  // Inserts (order, new_order) — skipped.

  // ---- Item reads: separate loop (Polyjuice acc_id 3) ----
  // Read all items first before touching stock, matching Polyjuice's loop
  // structure where all item reads complete before stock read+writes begin.
  int64_t i_prices[15];
  for (UInt32 ol_number = 0; ol_number < ol_cnt; ol_number++) {
    uint64_t ol_i_id = query->items[ol_number].ol_i_id;
    key = ol_i_id;
    item = index_read(_wl->i_item, key, 0);
    assert(item != NULL);
    row_t* r_item = ((row_t*)item->location);

    row_t* r_item_local = get_row(r_item, RD, op_cnt++);
    if (r_item_local == NULL) {
      return finish(Abort);
    }
    r_item_local->get_value(I_PRICE, i_prices[ol_number]);
  }

  // ---- Stock writes: separate loop (Polyjuice acc_ids 4+5) ----
  for (UInt32 ol_number = 0; ol_number < ol_cnt; ol_number++) {
    uint64_t ol_i_id = query->items[ol_number].ol_i_id;
    uint64_t ol_supply_w_id = query->items[ol_number].ol_supply_w_id;
    uint64_t ol_quantity = query->items[ol_number].ol_quantity;

    uint64_t stock_key = stockKey(ol_i_id, ol_supply_w_id);
    INDEX* stock_index = _wl->i_stock;
    itemid_t* stock_item;
    index_read(stock_index, stock_key, wh_to_part(ol_supply_w_id), stock_item);
    assert(stock_item != NULL);
    row_t* r_stock = ((row_t*)stock_item->location);
    row_t* r_stock_local = get_row(r_stock, WR, op_cnt++);
    if (r_stock_local == NULL) {
      return finish(Abort);
    }

    UInt64 s_quantity;
    int64_t s_remote_cnt;
    s_quantity = *(int64_t*)r_stock_local->get_value(S_QUANTITY);
#if !TPCC_SMALL
    int64_t s_ytd;
    int64_t s_order_cnt;
    r_stock_local->get_value(S_YTD, s_ytd);
    r_stock_local->set_value(S_YTD, s_ytd + ol_quantity);
    r_stock_local->get_value(S_ORDER_CNT, s_order_cnt);
    r_stock_local->set_value(S_ORDER_CNT, s_order_cnt + 1);
#endif
    if (remote) {
      s_remote_cnt = *(int64_t*)r_stock_local->get_value(S_REMOTE_CNT);
      s_remote_cnt++;
      r_stock_local->set_value(S_REMOTE_CNT, &s_remote_cnt);
    }
    uint64_t quantity;
    if (s_quantity > ol_quantity + 10) {
      quantity = s_quantity - ol_quantity;
    } else {
      quantity = s_quantity - ol_quantity + 91;
    }
    r_stock_local->set_value(S_QUANTITY, &quantity);

    // Order-line insert — skipped.
  }

  // op last: customer RD (Polyjuice acc_id 10, moved to end)
  key = custKey(c_id, d_id, w_id);
  index = _wl->i_customer_id;
  item = index_read(index, key, wh_to_part(w_id));
  assert(item != NULL);
  row_t* r_cust = (row_t*)item->location;
  row_t* r_cust_local = get_row(r_cust, RD, op_cnt++);
  if (r_cust_local == NULL) {
    return finish(Abort);
  }
  uint64_t c_discount;
  r_cust_local->get_value(C_DISCOUNT, c_discount);

  assert(rc == RCOK);
  return finish(rc);
}

// Stubs for unimplemented transaction types.
RC tpcc_txn_man::run_order_status(tpcc_query* query) { return ERROR; }
RC tpcc_txn_man::run_delivery(tpcc_query* query) { return ERROR; }
RC tpcc_txn_man::run_stock_level(tpcc_query* query) { return ERROR; }
