/*
 * LPThreadpool.h
 *
 *  Created on: Mar 19, 2014
 *      Author: lambros
 */

#ifndef LPTHREADPOOL_H_
#define LPTHREADPOOL_H_

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>


struct lp_tpjob{
	char dummy;
	void *args;
	void *(*func)(int, void *);
	lp_tpjob *next;
};

struct lp_threadpool{
	int workers_ids;
	int nthreads;
	int ncores;
	int pending_jobs;
	lp_tpjob *jobs_head;
	lp_tpjob *jobs_tail;

	pthread_cond_t cond_jobs;
	pthread_mutex_t mutex_pool;

	pthread_t *worker_threads;

	char *initialSleepThreads;

	int synced_threads;
	pthread_cond_t sleep;
	pthread_barrier_t pool_barrier;

	char headsTime;

	int threadpool_destroyed;
};


void lp_threadpool_destroy(lp_threadpool* pool);
lp_threadpool* lp_threadpool_init( int threads, int cores );
void lp_threadpool_addWorker(lp_threadpool *pool);
void lp_threadpool_startjobs(lp_threadpool* pool);
void lp_threadpool_addjob( lp_threadpool* pool, void *(*func)(int, void *), void* args );
void lp_threadpool_addjob_nolock( lp_threadpool* pool, void *(*func)(int, void *), void* args);
//lp_tpjob* lp_threadpool_fetchjob( lp_threadpool* pool );
void lp_threadpool_fetchjob( lp_threadpool* pool, lp_tpjob *njob );
int lp_threadpool_uniquetid( lp_threadpool* pool );
void* lp_tpworker_thread( void* _pool );
void synchronize_threads_master(int tid, void * arg);
void lp_threadpool_synchronize_master(lp_threadpool* pool);
void synchronize_complete(lp_threadpool* pool);
void lp_threadpool_destroy_threads(lp_threadpool*pool);

#endif /* LPTHREADPOOL_H_ */
