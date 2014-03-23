#ifndef __LP__SPARSEARRAY_LONG__
#define __LP__SPARSEARRAY_LONG__

#include <string.h>
#include <stdlib.h>

class LPSparseArrayLong{

public:
	LPSparseArrayLong();
	~LPSparseArrayLong();

	long set(long index, long val);
	long get(long index);
	long* getRef(long index);

	long* compress(long *arraySize) const;

	void clear();

private:
	const static long SPARSE_ARRAY_NODE_DATA = 1024;

	struct SparseArrayNode{
		SparseArrayNode(unsigned int low, SparseArrayNode*prev, SparseArrayNode*next){
			this->next = next;
			this->prev = prev;
			this->low = low;
			this->high = low + SPARSE_ARRAY_NODE_DATA - 1;
			memset(data, 0, SPARSE_ARRAY_NODE_DATA*sizeof(long));
		}
		SparseArrayNode* next;
		SparseArrayNode* prev;
		unsigned int low;
		unsigned int high;
		long data[SPARSE_ARRAY_NODE_DATA];
	};
	struct SparseArray{
		SparseArrayNode* head;
		SparseArrayNode* tail;
		unsigned int num_nodes;
		unsigned int mid_high;
	};

	SparseArray mSparseArray;
	long mInsertedElements;
};

#endif
