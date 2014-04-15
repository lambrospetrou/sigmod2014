/*
 * LPConcHashtable.h
 *
 *  Created on: Apr 15, 2014
 *      Author: lambros
 */

#ifndef LPCONCHASHTABLE_H_
#define LPCONCHASHTABLE_H_

#include "linkedlist.h"

class LPConcHashtable{

public:
	LPConcHashtable(long arraySize);
	~LPConcHashtable();

	int set(long key, long value);
	int inc(long key);

	int contains(long key);
	long get(long key);

	void printLists();
	void printBuckets();

private:

	long ArraySize;
	llist_t **ArrayLists;

	long MurmurHash3_64(long h){
		h ^= h >> 33;
		h *= 0xff51afd7ed558ccd;
		h ^= h >> 33;
		h *= 0xc4ceb9fe1a85ec53;
		h ^= h >> 33;
		return h;
	}

	long MurmurHash3_32(long h){
		h ^= h >> 16;
		h *= 0x85ebca6b;
		h ^= h >> 13;
		h *= 0xc2b2ae35;
		h ^= h >> 16;
		return h;
	}

};



#endif /* LPCONCHASHTABLE_H_ */
