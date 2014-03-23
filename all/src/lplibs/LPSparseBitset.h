/*
 * LPSparseBitset.h
 *
 *  Created on: Mar 23, 2014
 *      Author: lambros
 */

#ifndef LPSPARSEBITSET_H_
#define LPSPARSEBITSET_H_


#include <string.h>
#include <stdlib.h>

class LPSparseBitset{

public:
	LPSparseBitset();
	LPSparseBitset(unsigned long initialSize);
	~LPSparseBitset();

	void set(unsigned long index);
	bool isSet(unsigned long index) const;
	void clear(unsigned long index);
	void clearAll();

private:
	const static long SPARSE_ARRAY_NODE_DATA = 1024;
	const static long SPARSE_ARRAY_NODE_DATA_BITS = SPARSE_ARRAY_NODE_DATA << 3;

	struct SparseArrayNode{
		SparseArrayNode(unsigned long low){
			this->next = 0;
			this->prev = 0;
			this->low = low;
			this->high = low + SPARSE_ARRAY_NODE_DATA_BITS - 1;
			memset(data, 0, SPARSE_ARRAY_NODE_DATA*sizeof(unsigned char));
		}
		SparseArrayNode* next;
		SparseArrayNode* prev;
		unsigned int low;
		unsigned int high;
		unsigned char data[SPARSE_ARRAY_NODE_DATA];
	};
	struct SparseArray{
		SparseArrayNode* head;
		SparseArrayNode* tail;
		unsigned long num_nodes;
		unsigned long mid_high;
	};

	SparseArray mSparseArray;
};


#endif /* LPSPARSEBITSET_H_ */
