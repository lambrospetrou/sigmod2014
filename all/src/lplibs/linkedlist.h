/*
 *  linkedlist.h
 *  interface for the list
 *
 */
#ifndef LLIST_H_ 
#define LLIST_H_


#include <assert.h>
#include <getopt.h>
#include <limits.h>
#include <pthread.h>
#include <signal.h>
#include <stdlib.h>
#include <stdio.h>
#include <sys/time.h>
#include <time.h>
#include <stdint.h>

#include "atomic_ops_if.h"

#ifdef DEBUG
#define IO_FLUSH                        fflush(NULL)
#endif

typedef long val_t;

// TODO - cache block padding
typedef struct node 
{
	val_t value;
	val_t key;
	struct node *next;
	//uint8_t padding[64-sizeof(val_t)-sizeof(struct node*)];
} node_t;

typedef struct llist 
{
	node_t *head;
	node_t *tail;
	uint64_t size;
} llist_t;

inline int is_marked_ref(long i);
inline long unset_mark(long i);
inline long set_mark(long i);
inline long get_unmarked_ref(long w);
inline long get_marked_ref(long w);


llist_t* list_new();
//return 0 if not found, positive number otherwise
int list_contains(llist_t *the_list, val_t key);
//return 0 if value already in the list, positive number otherwise
int list_add(llist_t *the_list, val_t key);
int list_add_withValue(llist_t *the_list, val_t key, val_t value);

int list_inc(llist_t *the_list, val_t key);
long list_get(llist_t* the_list, val_t key);

void list_delete(llist_t *the_list);
int list_size(llist_t *the_list);


node_t* new_node(val_t val, node_t* next);
node_t* list_search(llist_t* the_list, val_t val, node_t** left_node);


#endif
