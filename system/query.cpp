#include "query.h"

#include <sched.h>

#include "mem_alloc.h"
#include "table.h"
#include "tpcc_helper.h"
#include "tpcc_query.h"
#include "wl.h"
#include "ycsb_query.h"

/*************************************************/
//     class Query_queue
/*************************************************/
int Query_queue::_next_tid;

// Pre-generates all queries before the benchmark begins.
// For YCSB: computes the Zipf denominator (zeta) once globally so per-thread
//   query generation can use it without recomputing.
// Spawns g_thread_cnt-1 helper threads (plus uses the caller itself) to
// generate queries in parallel, each thread building its own query array.
// Thread IDs are claimed with an atomic fetch-add so no two threads duplicate
// the same slot. (AI-generated)
void Query_queue::init(workload* h_wl) {
  all_queries = new Query_thd*[g_thread_cnt];
  _wl = h_wl;
  _next_tid = 0;

  if constexpr (wl == WL::Ycsb) {
    ycsb_query::calculateDenom();
  } else if constexpr (wl == WL::Tpcc) {
    assert(tpcc_buffer != NULL);
  }
  int64_t begin = get_server_clock();
  pthread_t p_thds[g_thread_cnt - 1];
  for (UInt32 i = 0; i < g_thread_cnt - 1; i++) {
    pthread_create(&p_thds[i], NULL, threadInitQuery, this);
  }
  threadInitQuery(this);
  for (uint32_t i = 0; i < g_thread_cnt - 1; i++) {
    pthread_join(p_thds[i], NULL);
  }
  int64_t end = get_server_clock();
  printf("Query Queue Init Time %f\n", 1.0 * (end - begin) / 1000000000UL);
}

void Query_queue::init_per_thread(int thread_id) {
  all_queries[thread_id] = (Query_thd*)_mm_malloc(sizeof(Query_thd), 64);
  new (all_queries[thread_id]) Query_thd();
  all_queries[thread_id]->init(_wl, thread_id);
}

base_query* Query_queue::get_next_query(uint64_t thd_id) {
  base_query* query = all_queries[thd_id]->get_next_query();
  return query;
}

void* Query_queue::threadInitQuery(void* This) {
  Query_queue* query_queue = (Query_queue*)This;
  uint32_t tid = ATOM_FETCH_ADD(_next_tid, 1);
  query_queue->init_per_thread(tid);
  return NULL;
}

/*************************************************/
//     class Query_thd
/*************************************************/

// Allocates and pre-generates all queries for one thread.
// request_cnt = warmup share + measurement share + small padding +
//               (if abort buffer enabled) extra slots so aborted queries can
//               be re-queued without wrapping around the array end.
//               (AI-generated)
void Query_thd::init(workload* h_wl, int thread_id) {
  q_idx = 0;

  [&]<WL W = wl>() {
    if constexpr (W == WL::Ycsb || W == WL::Tpcc) {
      uint64_t request_cnt;
      request_cnt = WARMUP / g_thread_cnt + MAX_TXN_PER_PART + 4;
      if constexpr (abort_buffer_enable) {
        request_cnt += ABORT_BUFFER_SIZE;
      }
      using QT = std::conditional_t<W == WL::Ycsb, ycsb_query, tpcc_query>;
      QT* q;
      if constexpr (W == WL::Ycsb) {
        q = (QT*)mem_allocator.alloc(sizeof(QT) * request_cnt, thread_id);
        buffer.init(thread_id + 1);
      } else {
        q = (QT*)_mm_malloc(sizeof(QT) * request_cnt, 64);
      }
      queries = reinterpret_cast<QueryType*>(q);
      for (UInt32 qid = 0; qid < request_cnt; qid++) {
        new (&q[qid]) QT();
        if constexpr (W == WL::Ycsb) {
          q[qid].init(thread_id, h_wl, this);
        } else {
          q[qid].init(thread_id, h_wl);
        }
      }
    }
  }();
}

base_query* Query_thd::get_next_query() {
  base_query* query = &queries[q_idx++];
  return query;
}
