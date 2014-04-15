/*
 * LPConcHashtable.cpp
 *
 *  Created on: Apr 15, 2014
 *      Author: lambros
 */

#include "LPConcHashtable.h"
#include <stdlib.h>

LPConcHashtable::~LPConcHashtable(){

}

LPConcHashtable::LPConcHashtable(long arraySize){
	this->ArraySize = arraySize;
	this->ArrayLists = (llist**)malloc(sizeof(llist)*arraySize);
	for( long i=0; i<arraySize; i++ ){
		this->ArrayLists[i] = list_new();
	}
}

int LPConcHashtable::set(long key, long value){
	//return list_add_withValue( this->ArrayLists[MurmurHash3_64(key) % this->ArraySize], key, value);
	return list_add_withValue( this->ArrayLists[key % this->ArraySize], key, value);
}

int LPConcHashtable::inc(long key){
	//return list_inc( this->ArrayLists[MurmurHash3_64(key) % this->ArraySize], key);
	return list_inc( this->ArrayLists[key % this->ArraySize], key);
}


long LPConcHashtable::get(long key){
	//return list_get(this->ArrayLists[MurmurHash3_64(key) % this->ArraySize ], key);
	return list_get(this->ArrayLists[key % this->ArraySize ], key);
}

int LPConcHashtable::contains(long key){
	//return list_contains(this->ArrayLists[MurmurHash3_64(key) % this->ArraySize], key);
	return list_contains(this->ArrayLists[key % this->ArraySize], key);
}

long LPConcHashtable::arraySize(){
	return this->ArraySize;
}

void LPConcHashtable::printBuckets(){
	for( long i=0; i<this->ArraySize; i++ ){
		fprintf(stderr, "list[%d] size[%d]\n", i, this->ArrayLists[i]->size);
	}
}

void LPConcHashtable::printLists(){
	for( long i=0; i<this->ArraySize; i++ ){
		fprintf(stderr, "\nlist[%ld] size[%ld]\n", i, this->ArrayLists[i]->size);
		for( node_t *j=this->ArrayLists[i]->head; j != NULL ; j=j->next ){
			fprintf(stderr, "[%ld:%ld] | ", j->key, j->value);
		}
	}
}
