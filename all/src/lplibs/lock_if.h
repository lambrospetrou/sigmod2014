/*
 * File: lock.if
 * Description: implements different lock algorithms
 */

#ifndef _LOCK_IF_H_
#define _LOCK_IF_H_

#include "atomic_ops_if.h"

typedef uint32_t ptlock_t;

static inline void
lock_init(volatile ptlock_t* l){
	*l = 0;
}

static inline void
lock_destroy(volatile ptlock_t* l){
	// not really needed
	*l = 666;
	// maybe should call unlock too
}


static inline uint32_t
lock_lock(volatile ptlock_t* l){
    uint32_t backoff;
	uint32_t maxbackoff = 1<<25;
    backoff = ((((uint64_t)&backoff) & 0x00000000000000ff) + 1) <<15;
    while(1) {
    	while (*l != 0) {}
        if( CAS_U32(l, 0, 1) == 0 ) {
        //if (TAS_U8(l) == 0) {
        	return 1;
        } else {
        	for(;backoff>0;backoff--);
            	backoff<<=1;
            backoff = (backoff < maxbackoff)?backoff:maxbackoff;
        }
    }
    asm volatile("" ::: "memory");
    return 0;
}

static inline uint32_t
lock_unlock(volatile ptlock_t* l){
	asm volatile("" ::: "memory");
    // no need to CAS since we are the only ones with access to this
    *l = 0;
    return 0;
}


/*
static inline uint32_t
lock_lock(volatile ptlock_t* l)
{
	while( CAS_U32(l, 0, 1) == 1 ) {}
	return 1;
}

static inline uint32_t
lock_unlock(volatile ptlock_t* l)
{
	// no need to CAS since we are the only ones with access to this
	*l = 0;
	return 0;
}
*/


#endif	/* _LOCK_IF_H_ */
