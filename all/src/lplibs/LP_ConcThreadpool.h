#ifndef LP_CONC_THREADPOOL_H_
#define LP_CONC_THREADPOOL_H_

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>

#include "lock_if.h"

struct lp_conc_tpjob{
	char dummy;
	void *args;
	void *(*func)(int, void *);
	lp_conc_tpjob *next;
};

struct lp_conc_threadpool{
	int workers_ids;
	int nthreads;
	int pending_jobs;
	lp_conc_tpjob *jobs_head;
	lp_conc_tpjob *jobs_tail;

	//pthread_cond_t cond_jobs;
	//pthread_mutex_t mutex_pool;
	ptlock_t mutex_pool;
	int cond_jobs;

	pthread_t *worker_threads;

	int synced_threads;
	//pthread_cond_t sleep;
	//pthread_barrier_t pool_barrier;

	char headsTime;
};


void lp_conc_threadpool_destroy(lp_conc_threadpool* pool);
lp_threadpool* lp_conc_threadpool_init( int threads );
void lp_conc_threadpool_addjob( lp_conc_threadpool* pool, void *(*func)(int, void *), void* args );
//lp_tpjob* lp_threadpool_fetchjob( lp_conc_threadpool* pool );
void lp_conc_threadpool_fetchjob( lp_conc_threadpool* pool, lp_conc_tpjob *njob );
int lp_conc_threadpool_uniquetid( lp_conc_threadpool* pool );
void* lp_conc_tpworker_thread( void* _pool );
void synchronize_threads_master(int tid, void * arg);
void lp_conc_threadpool_synchronize_master(lp_conc_threadpool* pool);
void synchronize_complete(lp_conc_threadpool* pool);


#endif /* LP_CONC_THREADPOOL_H_ */
