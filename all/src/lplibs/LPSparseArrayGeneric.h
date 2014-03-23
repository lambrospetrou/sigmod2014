#ifndef __LP__SPARSEARRAY_GENERIC__
#define __LP__SPARSEARRAY_GENERIC__

#include <string.h>
#include <stdlib.h>

template<class T>
class LPSparseArrayGeneric{

public:
	LPSparseArrayGeneric();
	~LPSparseArrayGeneric();

	void set(unsigned long index, T val);
	T get(unsigned long index);
	T* getRef(unsigned long index);

	void clear();

private:
	const static long SPARSE_ARRAY_NODE_DATA = 4096;

	struct SparseArrayNode{
		SparseArrayNode(unsigned int low){
			this->next = 0;
			this->prev = 0;
			this->low = low;
			this->high = low + SPARSE_ARRAY_NODE_DATA - 1;
			memset(data, 0, SPARSE_ARRAY_NODE_DATA*sizeof(T));
		}
		SparseArrayNode* next;
		SparseArrayNode* prev;
		unsigned int low;
		unsigned int high;
		T data[SPARSE_ARRAY_NODE_DATA];
	};
	struct SparseArray{
		SparseArrayNode* head;
		SparseArrayNode* tail;
		unsigned long num_nodes;
		unsigned long mid_high;
	};

	SparseArray mSparseArray;
};

template<class T>
LPSparseArrayGeneric<T>::LPSparseArrayGeneric(){
	this->mSparseArray.head = mSparseArray.tail = new SparseArrayNode(0);
	this->mSparseArray.num_nodes = 1;
	this->mSparseArray.mid_high = mSparseArray.head->high;
}

template<class T>
LPSparseArrayGeneric<T>::~LPSparseArrayGeneric(){
	for( SparseArrayNode* prev; mSparseArray.head; mSparseArray.head=prev ){
		prev = mSparseArray.head->next;
		free( mSparseArray.head );
	}
}

template<class T>
void LPSparseArrayGeneric<T>::set(unsigned long index, T value) {
	SparseArrayNode* prev = mSparseArray.head, *cnode;
	if (index < mSparseArray.mid_high) {
		// START FROM THE HEAD AND SEARCH FORWARD

		for (cnode = mSparseArray.head; cnode && cnode->high < index; cnode = cnode->next) {
			prev = cnode;
		}
		// the case where we finished the array without results
		// OR
		// we must create a node before this one because this is a block for bigger ids
		if (cnode == 0 || cnode->low > index) {
			cnode = new SparseArrayNode((index / SPARSE_ARRAY_NODE_DATA) * SPARSE_ARRAY_NODE_DATA);
			cnode->next = prev->next;
			cnode->prev = prev;
			prev->next = cnode;
			if( cnode->next != 0 ){
				cnode->next->prev = cnode;
			}
			mSparseArray.num_nodes++;
			//if( sa->num_nodes % 2 == 1 ){
			if ((mSparseArray.num_nodes & 1) == 1) {
				// we must increase the mid_high
				unsigned int i = 0, sz = (mSparseArray.num_nodes >> 2);
				for (prev = mSparseArray.head; i < sz; i++) {
					prev = prev->next;
				}
				mSparseArray.mid_high = prev->high;
			}
		}
	} else {
		// START FROM THE TAIL AND SEARCH BACKWARD
		for (cnode = mSparseArray.tail; cnode->low > index; cnode =	cnode->prev) {
			// move back
		}
		// the case where we stopped at a node with HIGH less than index and we must create a new node cause LOW is also less than index
		// OR
		// the case where we stopped at the right node
		if (cnode->high < index) {
			prev = cnode;
			cnode = new SparseArrayNode((index / SPARSE_ARRAY_NODE_DATA) * SPARSE_ARRAY_NODE_DATA);
			if (prev == mSparseArray.tail) {
				mSparseArray.tail = cnode;
			}
			cnode->next = prev->next;
			cnode->prev = prev;
			prev->next = cnode;
			if (cnode->next != 0) {
				cnode->next->prev = cnode;
			}
			mSparseArray.num_nodes++;
			//if (sa->num_nodes % 2 == 1) {
			if ((mSparseArray.num_nodes & 1) == 1) {
				// we must increase the mid_high
				unsigned int i = 0, sz = (mSparseArray.num_nodes >> 2);
				for (prev = mSparseArray.head; i < sz; i++) {
					prev = prev->next;
				}
				mSparseArray.mid_high = prev->high;
			}

		}
	}
	// cnode holds the block where we need to insert the value
	cnode->data[index - cnode->low] = value;
	//fprintf( stderr, "qid[%u] pos[%d] sa_index[%u] sa_low[%u] sa_high[%u]\n", index, pos, index - sa->low, sa->low, sa->high );
}

template<class T>
T* LPSparseArrayGeneric<T>::getRef(unsigned long index){
	SparseArrayNode *cnode;
	if (index < mSparseArray.mid_high) {
		// START FROM THE HEAD AND SEARCH FORWARD
		for (cnode = mSparseArray.head; cnode && cnode->high < index; cnode = cnode->next) {
			// go forward
		}
		// the case where we finished the array without finding the required section
		if (cnode == 0 || cnode->low > index) {
			return 0;
		}
	} else {
		// START FROM THE TAIL AND SEARCH BACKWARD
		for (cnode = mSparseArray.tail; cnode->low > index; cnode =	cnode->prev) {
			// move back
		}
		// the case where we stopped at a node with HIGH less than index
		if (cnode->high < index) {
			return 0;
		}
	}
	// cnode holds the block where we have the required value
	return &cnode->data[index - cnode->low];
}

template<class T>
T LPSparseArrayGeneric<T>::get(unsigned long index){
	T* found = getRef(index);
	if( found == 0 )
		return 0;
	return *found;
}

template<class T>
void LPSparseArrayGeneric<T>::clear(){
	for (SparseArrayNode *cnode = mSparseArray.head; cnode; cnode = cnode->next) {
		memset(cnode->data, 0, SPARSE_ARRAY_NODE_DATA*sizeof(T));
	}
}

#endif
