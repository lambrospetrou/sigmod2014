/*
 * LPSparseBitset.cpp
 *
 *  Created on: Mar 23, 2014
 *      Author: lambros
 */


#include "LPSparseBitset.h"

/*
 * Returns the number of bytes required to hold N bits
 */
static inline long getRequiredBytes(long n){
	long i=n>>3; // n/8
	return (i<<8 == n)?i:i+1;
}

/*
 * Returns the index of the byte that contains this bit
 */
static inline long getByteForBit(long bit){
	return bit>>3; // bit/8
}

/*
 * Returns a byte with ace(1) in the position of the bit.
 *
 * 	pos = bit % 8 - assuming pos=3 = 00001000
 *
 */
static inline long getPosForBit(long bit){
	return 1 << (bit & 0x7); // 1 << (bit%8)
}

LPSparseBitset::LPSparseBitset(){
	this->mSparseArray.head = mSparseArray.tail = new SparseArrayNode(0);
	this->mSparseArray.num_nodes = 1;
	this->mSparseArray.mid_high = mSparseArray.head->high;
}


LPSparseBitset::~LPSparseBitset(){
	for( SparseArrayNode* prev; mSparseArray.head; mSparseArray.head=prev ){
		prev = mSparseArray.head->next;
		free( mSparseArray.head );
	}
}

void LPSparseBitset::set(long index, char bitValue) {
	SparseArrayNode *cnode, *prev;
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
	long bytePos = getByteForBit(index - cnode->low);
	cnode->data[bytePos] = cnode->data[bytePos] | getPosForBit(index);
	//fprintf( stderr, "qid[%u] pos[%d] sa_index[%u] sa_low[%u] sa_high[%u]\n", index, pos, index - sa->low, sa->low, sa->high );
}

void LPBitset::set(long indexBit){
	this->mBytes[getByteForBit(indexBit)] = this->mBytes[getByteForBit(indexBit)] | getPosForBit(indexBit);
}

bool LPBitset::isSet(long indexBit) const{
	return (this->mBytes[getByteForBit(indexBit)] & getPosForBit(indexBit) ) > 0;
}

void LPBitset::clear(long indexBit){
	char mask = ~getPosForBit(indexBit);
	this->mBytes[getByteForBit(indexBit)] &= mask;
}

char LPSparseBitset::isSet(long index) const{
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
		// the case where we stopped at a node with HIGH less than index and we must create a new node cause LOW is also less than index
		// OR
		// the case where we stopped at the right node
		if (cnode->high < index) {
			return 0;
		}
	}
	// cnode holds the block where we have the required value
	long bytePos = getByteForBit(index - cnode->low);
	return (cnode->data[getByteForBit(bytePos)] & getPosForBit(index) ) > 0;
}

void LPSparseBitset::clear(long index){
	SparseArrayNode *cnode;
	if (index < mSparseArray.mid_high) {
		// START FROM THE HEAD AND SEARCH FORWARD
		for (cnode = mSparseArray.head; cnode && cnode->high < index; cnode =
				cnode->next) {
			// go forward
		}
		// the case where we finished the array without finding the required section
		if (cnode == 0 || cnode->low > index) {
			return;
		}
	} else {
		// START FROM THE TAIL AND SEARCH BACKWARD
		for (cnode = mSparseArray.tail; cnode->low > index; cnode =
				cnode->prev) {
			// move back
		}
		// the case where we stopped at a node with HIGH less than index and we must create a new node cause LOW is also less than index
		// OR
		// the case where we stopped at the right node
		if (cnode->high < index) {
			return;
		}
	}
	// cnode holds the block where we have the required value
	long bytePos = getByteForBit(index - cnode->low);
	char mask = ~getPosForBit(index);
	cnode->data[getByteForBit(bytePos)] &= mask;
}

void LPSparseBitset::clearAll(){
	for (SparseArrayNode*cnode = mSparseArray.head; cnode; cnode = cnode->next) {
		memset(cnode->data, 0, SPARSE_ARRAY_NODE_DATA);
	}
}
