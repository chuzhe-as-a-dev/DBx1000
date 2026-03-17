#include "cc_hooks.h"
#include "global.h"
#include "manager.h"
#include "mem_alloc.h"
#include "occ.h"
#include "plock.h"
#include "query.h"
#include "test.h"
#include "thread.h"
#include "tpcc.h"
#include "vll.h"
#include "ycsb.h"

void* worker_thread_entry(void*);

thread_t** g_threads;

// defined in parser.cpp
void parser(int argc, char* argv[]);

int main(int argc, char* argv[]) {
  parser(argc, argv);

  mem_allocator.init(g_part_cnt, MEM_SIZE / g_part_cnt);
  stats.init();
  glob_manager = (Manager*)_mm_malloc(sizeof(Manager), 64);
  glob_manager->init();
  if constexpr (cc_alg == CCAlg::DlDetect) {
    dl_detector.init();
  }
  printf("mem_allocator initialized!\n");
  workload* m_wl;
  if constexpr (wl == WL::Ycsb) {
    m_wl = new ycsb_wl;
  } else if constexpr (wl == WL::Tpcc) {
    m_wl = new tpcc_wl;
  } else if constexpr (wl == WL::Test) {
    m_wl = new TestWorkload;
    ((TestWorkload*)m_wl)->tick();
  } else {
    assert(false);
    m_wl = nullptr;
  }
  m_wl->init();
  printf("workload initialized!\n");

  uint64_t thd_cnt = g_thread_cnt;
  pthread_t p_thds[thd_cnt - 1];
  g_threads = new thread_t*[thd_cnt];
  for (uint32_t i = 0; i < thd_cnt; i++) {
    g_threads[i] = (thread_t*)_mm_malloc(sizeof(thread_t), 64);
  }
  // query_queue should be the last one to be initialized!!!
  // because it collects txn latency
  query_queue = (Query_queue*)_mm_malloc(sizeof(Query_queue), 64);
  if constexpr (wl != WL::Test) {
    query_queue->init(m_wl);
  }
  warmup_bar = std::make_unique<std::barrier<>>(g_thread_cnt);
  printf("query_queue initialized!\n");
  if constexpr (cc_alg == CCAlg::Hstore) {
    part_lock_man.init();
  } else if constexpr (cc_alg == CCAlg::Occ) {
    occ_man.init();
  } else if constexpr (cc_alg == CCAlg::Vll) {
    vll_man.init();
  } else if constexpr (cc_alg == CCAlg::PerOp) {
    cc_global_init();
  }

  for (uint32_t i = 0; i < thd_cnt; i++) {
    g_threads[i]->init(i, m_wl);
  }

  if (WARMUP > 0) {
    printf("WARMUP start!\n");
    for (uint32_t i = 0; i < thd_cnt - 1; i++) {
      uint64_t vid = i;
      pthread_create(&p_thds[i], NULL, worker_thread_entry, (void*)vid);
    }
    worker_thread_entry((void*)(thd_cnt - 1));
    for (uint32_t i = 0; i < thd_cnt - 1; i++) {
      pthread_join(p_thds[i], NULL);
    }
    printf("WARMUP finished!\n");
  }
  warmup_finish = true;
  warmup_bar = std::make_unique<std::barrier<>>(g_thread_cnt);

  // spawn and run txns again.
  int64_t starttime = get_server_clock();
  for (uint32_t i = 0; i < thd_cnt - 1; i++) {
    uint64_t vid = i;
    pthread_create(&p_thds[i], NULL, worker_thread_entry, (void*)vid);
  }
  worker_thread_entry((void*)(thd_cnt - 1));
  for (uint32_t i = 0; i < thd_cnt - 1; i++) {
    pthread_join(p_thds[i], NULL);
  }
  int64_t endtime = get_server_clock();

  if constexpr (wl != WL::Test) {
    printf("PASS! SimTime = %ld\n", endtime - starttime);
    if constexpr (stats_enable) {
      stats.print();
    }
  } else {
    ((TestWorkload*)m_wl)->summarize();
  }
  return 0;
}

void* worker_thread_entry(void* id) {
  uint64_t tid = (uint64_t)id;
  g_threads[tid]->run();
  return NULL;
}
