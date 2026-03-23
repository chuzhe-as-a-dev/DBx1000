#include "manager.h"
#include "row.h"
#include "txn.h"
#include "pthread.h"

void Manager::init() {
	timestamp = (uint64_t *) _mm_malloc(sizeof(uint64_t), 64);
	*timestamp = 1;
	_last_min_ts_time = 0;
	_min_ts = 0;
	_epoch = (uint64_t *) _mm_malloc(sizeof(uint64_t), 64);
	_last_epoch_update_time = (ts_t *) _mm_malloc(sizeof(uint64_t), 64);
	_epoch = 0;
	_last_epoch_update_time = 0;
	all_ts = (ts_t volatile **) _mm_malloc(sizeof(ts_t *) * g_thread_cnt, 64);
	for (uint32_t i = 0; i < g_thread_cnt; i++) 
		all_ts[i] = (ts_t *) _mm_malloc(sizeof(ts_t), 64);

	_all_txns = new txn_man * [g_thread_cnt];
	for (UInt32 i = 0; i < g_thread_cnt; i++) {
		*all_ts[i] = UINT64_MAX;
		_all_txns[i] = NULL;
	}
	for (UInt32 i = 0; i < BUCKET_CNT; i++)
		pthread_mutex_init( &mutexes[i], NULL );
}

// Allocates a globally unique, monotonically increasing timestamp.
// Four strategies (selected at compile/runtime via g_ts_alloc):
//   TS_MUTEX  – mutex-protected increment; simple but highest contention.
//   TS_CAS    – lock-free atomic fetch-add; supports batch allocation
//               (reserves g_ts_batch_num timestamps per call) to amortise
//               the atomic op cost when many threads are active.
//   TS_HW     – Graphite hardware counter (not available in NOGRAPHITE builds).
//   TS_CLOCK  – wall-clock * thread_cnt + thread_id; gives globally unique
//               values without any shared state, at the cost of being tied
//               to real time. (AI-generated)
uint64_t
Manager::get_ts(uint64_t thread_id) {
	if (g_ts_batch_alloc)
		assert(g_ts_alloc == TS_CAS);
	uint64_t time;
	uint64_t starttime = get_sys_clock();
	switch(g_ts_alloc) {
	case TS_MUTEX :
		pthread_mutex_lock( &ts_mutex );
		time = ++(*timestamp);
		pthread_mutex_unlock( &ts_mutex );
		break;
	case TS_CAS :
		if (g_ts_batch_alloc)
			time = ATOM_FETCH_ADD((*timestamp), g_ts_batch_num);
		else
			time = ATOM_FETCH_ADD((*timestamp), 1);
		break;
	case TS_HW :
#ifndef NOGRAPHITE
		time = CarbonGetTimestamp();
#else
		assert(false);
		exit(-1);
#endif
		break;
	case TS_CLOCK :
		time = get_sys_clock() * g_thread_cnt + thread_id;
		break;
	default :
		assert(false);
		exit(-1);
	}
	INC_STATS(thread_id, time_ts_alloc, get_sys_clock() - starttime);
	return time;
}

// Returns the minimum active timestamp across all threads, used by MVCC to
// determine which old versions can be garbage collected. Only thread 0
// refreshes the cached value (at most once per MIN_TS_INTVL) to avoid
// O(threads) work on every call. The cached value is monotonically
// non-decreasing so stale reads are safe — they just delay GC slightly. (AI-generated)
ts_t Manager::get_min_ts(uint64_t tid) {
	uint64_t now = get_sys_clock();
	uint64_t last_time = _last_min_ts_time;
	if (tid == 0 && now - last_time > MIN_TS_INTVL)
	{
		ts_t min = UINT64_MAX;
    		for (UInt32 i = 0; i < g_thread_cnt; i++)
			if (*all_ts[i] < min)
		    	    	min = *all_ts[i];
		if (min > _min_ts)
			_min_ts = min;
	}
	return _min_ts;
}

void Manager::add_ts(uint64_t thd_id, ts_t ts) {
	assert( ts >= *all_ts[thd_id] || 
		*all_ts[thd_id] == UINT64_MAX);
	*all_ts[thd_id] = ts;
}

void Manager::set_txn_man(txn_man * txn) {
	int thd_id = txn->get_thd_id();
	_all_txns[thd_id] = txn;
}


uint64_t Manager::hash(row_t * row) {
	uint64_t addr = (uint64_t)row / MEM_ALLIGN;
    return (addr * 1103515247 + 12345) % BUCKET_CNT;
}
 
void Manager::lock_row(row_t * row) {
	int bid = hash(row);
	pthread_mutex_lock( &mutexes[bid] );	
}

void Manager::release_row(row_t * row) {
	int bid = hash(row);
	pthread_mutex_unlock( &mutexes[bid] );
}
	
// Advances the global epoch used by SILO. SILO assigns each committed
// transaction a TID that encodes the current epoch, so all threads must agree
// on epoch boundaries. The epoch is bumped at most once per LOG_BATCH_TIME ms
// to amortise the cost; any thread can call this but only one will win the
// time-check race. (AI-generated)
void
Manager::update_epoch()
{
	ts_t time = get_sys_clock();
	if (time - *_last_epoch_update_time > LOG_BATCH_TIME * 1000 * 1000) {
		*_epoch = *_epoch + 1;
		*_last_epoch_update_time = time;
	}
}
