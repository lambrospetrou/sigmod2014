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
	~LPSparseBitset();

	void set(long index, char bitValue);
	char isSet(long index) const;
	void clear(long index);
	void clearAll();

private:
	const static long SPARSE_ARRAY_NODE_DATA = 4096;

	struct SparseArrayNode{
		SparseArrayNode(unsigned long low){
			this->next = 0;
			this->prev = 0;
			this->low = low;
			this->high = low + (SPARSE_ARRAY_NODE_DATA << 3) - 1;
			memset(data, 0, SPARSE_ARRAY_NODE_DATA*sizeof(char));
		}
		SparseArrayNode* next;
		SparseArrayNode* prev;
		unsigned int low;
		unsigned int high;
		char data[SPARSE_ARRAY_NODE_DATA];
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
