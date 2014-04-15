/*
 *  linkedlist.c
 *
 *  Description:
 *   Lock-free linkedlist implementation of Harris' algorithm
 *   "A Pragmatic Implementation of Non-Blocking Linked Lists" 
 *   T. Harris, p. 300-314, DISC 2001.
 */

#include "linkedlist.h"
#include <stdint.h>
#include <limits.h>

/*
 * The five following functions handle the low-order mark bit that indicates
 * whether a node is logically deleted (1) or not (0).
 *  - is_marked_ref returns whether it is marked, 
 *  - (un)set_marked changes the mark,
 *  - get_(un)marked_ref sets the mark before returning the node.
 */
	inline int
is_marked_ref(long i) 
{
	return (int) (i & 0x1L);
}

	inline long
unset_mark(long i)
{
	i &= ~0x1L;
	return i;
}

	inline long
set_mark(long i) 
{
	i |= 0x1L;
	return i;
}

	inline long
get_unmarked_ref(long w) 
{
	return w & ~0x1L;
}

	inline long
get_marked_ref(long w) 
{
	return w | 0x1L;
}



void printList(llist_t * the_list){
	node_t*c;
	for(c=the_list->head->next; c!=the_list->tail; c=(node_t*)get_unmarked_ref((long)c->next)){
		fprintf(stderr, "%c%d ", (is_marked_ref((long)c->next)?'!':'='), (int)c->value);
	}
}


/*
 * list_search looks for value val, it
 *  - returns right_node owning val (if present) or its immediately higher 
 *    value present in the list (otherwise) and 
 *  - sets the left_node to the node owning the value immediately lower than val. 
 * Encountered nodes that are marked as logically deleted are physically removed
 * from the list, yet not garbage collected.
 */
node_t* list_search(llist_t* set, val_t val, node_t** left) 
{
	node_t *left_next=NULL, *right;
	node_t *t, *t_next;
search_again:
	do{
		t = set->head;
		t_next = set->head->next;
		// 1: find left and right node
		do{
			if(!is_marked_ref((long)t_next)){
				*left = t;
				left_next = t_next;
			}
			t = (node_t*)get_unmarked_ref((long)t_next);
			if(t==set->tail) break;
			t_next = t->next;
		}while(is_marked_ref((long)t_next) || (t->key<val));
		right = t;
		// 2: check if nodes are adjacent
		if( left_next == right ){
			if( right != set->tail && is_marked_ref((long)right->next) ){
				goto search_again;
			}else{
				return right;
			}
		}
		// 3: remove one or more marked nodes
		if( CAS_PTR(&((*left)->next), left_next, right) == left_next ){
			if( right != set->tail && is_marked_ref((long)right->next) ){
				goto search_again;
			}else{
				return right;
			}
		}

	}while(1);
	return NULL;
}


/*
 * list_contains returns a value different from 0 whether there is a node in the list owning value val.
 */
int list_contains(llist_t* the_list, val_t key)
{	
	node_t *c = the_list->head->next, *t;
	for(; c->next != NULL; ){
		if( is_marked_ref((long)c->next) ){
			t = (node_t*)get_unmarked_ref((long)c->next);
			c = t;
		}else{
			if( c->key < key ){
				t = (node_t*)get_unmarked_ref((long)c->next);
				c = t;
			}else{
				break;
			}
		}
	}
	if( !is_marked_ref((long)c->next) && c->key == key ){
		return 1;
	}else{
		return 0;
	}
	
	node_t *right, *left=NULL;
	right = list_search(the_list, key, &left);
	if( (right==the_list->tail) || (right->key != key) ){
		//fprintf(stderr, "\ncontains failure[%d]\n", val);
		//printList(the_list);
		return 0;
	}else{
		//fprintf(stderr, "\ncontains success[%d]\n", val);
		//printList(the_list);
		return 1;
	}
}


/*
 * list_contains returns the value of a node in the list with key.
 * otherwise -1
 */
long list_get(llist_t* the_list, val_t key)
{
	node_t *c = the_list->head->next, *t;
	for(; c->next != NULL; ){
		if( is_marked_ref((long)c->next) ){
			t = (node_t*)get_unmarked_ref((long)c->next);
			c = t;
		}else{
			if( c->key < key ){
				t = (node_t*)get_unmarked_ref((long)c->next);
				c = t;
			}else{
				break;
			}
		}
	}
	if( !is_marked_ref((long)c->next) && c->key == key ){
		return c->value;
	}else{
		return -1;
	}

	node_t *right, *left=NULL;
	right = list_search(the_list, key, &left);
	if( (right==the_list->tail) || (right->key != key) ){
		//fprintf(stderr, "\ncontains failure[%d]\n", val);
		//printList(the_list);
		return -1;
	}else{
		//fprintf(stderr, "\ncontains success[%d]\n", val);
		//printList(the_list);
		return right->value;
	}
}

/*
 * list_add inserts a new node with the given value val in the list
 * (if the value was absent) or does nothing (if the value is already present).
 */
int list_add(llist_t *the_list, val_t key)
{
	//fprintf(stderr, "\nadd[%d]\n", val);
	//printList(the_list);

	node_t* n = (node_t*)malloc(sizeof(node_t));
	n->key = key;
	n->value = 0;
	
	node_t *left=NULL, *right;
	do{
		right = list_search(the_list, key, &left);
		if( (right != the_list->tail) && (right->key == key) ){
	//fprintf(stderr, "\nadd[%d] finished\n", val);
	//printList(the_list);
			free(n);
			return 0;
		}
		n->next = right;
		if( CAS_PTR(&left->next, right, n) == right ){
			FAI_U64(&the_list->size);
	//fprintf(stderr, "\nadd[%d] finished\n", val);
	//printList(the_list);
			return 1;
		}
	}while(1);
	return 0;
}

/*
 * list_add inserts a new node with the given value val in the list
 * (if the value was absent) or does nothing (if the value is already present).
 */
int list_add_withValue(llist_t *the_list, val_t key, val_t value)
{
	//fprintf(stderr, "\nadd[%d]\n", val);
	//printList(the_list);

	node_t* n = (node_t*)malloc(sizeof(node_t));
	n->key = key;
	n->value = value;

	node_t *left=NULL, *right;
	do{
		right = list_search(the_list, key, &left);
		if( (right != the_list->tail) && (right->key == key) ){
	//fprintf(stderr, "\nadd[%d] finished\n", val);
	//printList(the_list);
			free(n);
			return 0;
		}
		n->next = right;
		if( CAS_PTR(&left->next, right, n) == right ){
			FAI_U64(&the_list->size);
	//fprintf(stderr, "\nadd[%d] finished\n", val);
	//printList(the_list);
			return 1;
		}
	}while(1);
	return 0;
}


int list_inc(llist_t *the_list, val_t key){
	//fprintf(stderr, "\ninc[%d]\n", val);
	node_t* n = (node_t*)malloc(sizeof(node_t));
	n->key = key;
	n->value = 1;

	node_t *left=NULL, *right;
	do{
		right = list_search(the_list, key, &left);
		if( (right != the_list->tail) && (right->key == key) ){
	//fprintf(stderr, "\nadd[%d] finished\n", val);
	//printList(the_list);
			FAI_U64(&right->value);
			free(n);
			return 0;
		}
		n->next = right;
		if( CAS_PTR(&left->next, right, n) == right ){
			FAI_U64(&the_list->size);
	//fprintf(stderr, "\nadd[%d] finished\n", val);
	//printList(the_list);
			return 1;
		}
	}while(1);
	return 0;
}

// not called besides new list
node_t* new_node(val_t key, node_t *next)
{
	node_t* node = (node_t*)malloc(sizeof(node_t));
	//node->value = 0;
	node->key = key;
	node->next = next;
	//fprintf(stderr, "\nsize[%d]\n", sizeof(node_t));
	return node;
}

llist_t* list_new()
{
	llist_t* the_list = (llist_t*)malloc(sizeof(llist_t));
	the_list->tail = new_node(LONG_MAX, NULL);
	the_list->head = new_node(LONG_MIN, the_list->tail);
	the_list->size = 0;
	return the_list;
}

void list_delete(llist_t *the_list)
{
	// TODO - NOT CALLED BUT IMPLEMENT IT LATER
}

int list_size(llist_t* the_list)
{
	return the_list->size;
}
