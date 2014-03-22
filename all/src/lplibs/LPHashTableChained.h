/*
 * @author: Lambros Petrou
 * @date: 11/2011
 * @last-modified: 27-11-2011
 * @website: www.lambrospetrou.com
 * @info: 
 *
 */

#ifndef LP_LPHASHTABLE_CHAINED_H
#define LP_LPHASHTABLE_CHAINED_H

///////////////////
// MACROS
///////////////////

// default size for the container that will be used to hold the chained structures = slot numbers
#define CONTAINER_SIZE 5003

// called when memory allocation fails, prints an error message and exits
#define noMemory(x) cerr << "Memory Allocation Failed! .::. "##x << endl; exit(1);

///////////////////
// HEADER INCLUTIONS
///////////////////
#include "LPList.h"
#include "LPVector.h"
#include <cstdlib>
#include <iostream>
#include <string>
#include <stdexcept>
#include <exception>
#include <new>
#include <locale>

using std::cerr;
using std::endl;
using std::string;

//////////////////////////////////////////////////////////////
////////////      DECLERATION OF LPHashTableChained   ///////////////////
//////////////////////////////////////////////////////////////

///////////////////
// BASE CLASS DECLARATIONS
// useful when specializing functions for specific types
///////////////////

// base class for all types
template<class K, unsigned int containerSize>
class LPHashTableChainedBase{
protected:
	// returns an index for a slot where the new Data will be placed
	unsigned int hashFunction(const K& key){
		return sizeof(key)*containerSize % containerSize;
	}
};

// specialization for string class
template<unsigned int containerSize>
class LPHashTableChainedBase<string, containerSize>{
protected:
	// returns an index for a slot where the new Data will be placed
	// specialized for string key
	unsigned int hashFunction(const string& key){
		unsigned int idx = 0;
		const char *ptr = NULL;
		unsigned int val = 0;
		ptr = key.c_str();

		while (*ptr != '\0') {
			int tmp;
			val = (val << 4) + (*ptr);
			if (tmp = (val & 0xf0000000)) {
			val = val ^ (tmp >> 24);
			val = val ^ tmp;
		}
			ptr++;
		}
		idx = val % containerSize;

		return idx;
	}
};

///////////////////
// MAIN CLASS DECLARATIONS
// useful when specializing functions for specific types
///////////////////
template<class K, class T, unsigned int containerSize=CONTAINER_SIZE>
class LPHashTableChained : public LPHashTableChainedBase<K,containerSize> {
	// PUBLIC
public:
///////////////////
// PUBLIC DECLARATIONS
///////////////////


///////////////////
// Constructors & Destructor
///////////////////
	
	// default constructor
	LPHashTableChained();

	// copy constructor
	LPHashTableChained(const LPHashTableChained& other);

	// destructor
	virtual ~LPHashTableChained();

///////////////////
// Accessors
///////////////////

	// get the element specified by key, if not eligible key return NULL
	T* search(const K& key);

	// return the size of the list
	unsigned int size()const;

	// returns true if empty
	bool empty()const;

	// returns a vector with all the elements inside in unordered sequence
	LPVector<T> getElements()const;

///////////////////
// Modifiers
///////////////////

	// insert the unique newData (type T) into the slot specified by key (type K)
	// (if it exists it isn't added again). Returns the address of newData or 
	// 0 if function failed to insert the element
	T* insert(const K& key, const T& newData);

	// deletes the deleteData of type T specified by the key of type K
	// if it does not exist does nothing
	bool erase(const K& key);

	// deletes all the elements and deallocates their memory
	// the slot container remains to have its memory
	void clear();

///////////////////
// Output
///////////////////


///////////////////
// Operator overloads
///////////////////

	// get the element specified by key, if not eligible index return NULL
	// calls search()
	T* operator[](const K& key);

	// delete the old elements and copy from other into this
	LPHashTableChained<K,T,containerSize>& operator=(const LPHashTableChained& other);

	// PRIVATE
private:
	class Element{
	public:
		K key;
		T* data;

		Element(const K& key, T* newData=0):key(key),data(newData){}
		Element(const Element& other){
			key = other.key;
			data = new T( *(other.data) );
		}

		bool operator==(const Element& other){
			return key == other.key;
		}
		
		bool operator!=(const Element& other){
			return key != other.key;
		}
		
		Element& operator=(const Element& other){
			key = other.key;
			if( data ) delete data;
			data = new T(*(other.data));
		}

		virtual ~Element(){
			if( data ) delete data;
			data=0;
		}
	};

	LPList<Element>* slot;
	unsigned int cSize;
};

//////////////////////////////////////////////////////////////
////////////      IMPLEMENTATION OF LPHashTableChained   ///////////////////
//////////////////////////////////////////////////////////////


///////////////////
// Constructors & Destructor
///////////////////
	
// default constructor
template<class K, class T, unsigned int containerSize>
LPHashTableChained<K,T,containerSize>::LPHashTableChained() : LPHashTableChainedBase(){
	cSize = 0;
	try{
		slot = new LPList<Element>[containerSize];
	}catch(std::bad_alloc&){
		noMemory("LPHashTableChained<K,T,Hash,containerSize>::LPHashTableChained()");
	}
}

// copy constructor
template<class K, class T, unsigned int containerSize>
LPHashTableChained<K,T,containerSize>::LPHashTableChained( const LPHashTableChained& other  ) : LPHashTableChainedBase(){
	cSize = other.cSize;
	try{
		slot = new LPList<Element>[containerSize];
	}catch(std::bad_alloc&){
		noMemory("LPHashTableChained<K,T,Hash,containerSize>::LPHashTableChained(other)");
	}
	// copy each list from other to this table
	for( unsigned int i=0; i< containerSize; i++ ){
		slot[i] = other.slot[i];
	}
}

// destructor
template<class K, class T, unsigned int containerSize>
LPHashTableChained<K,T,containerSize>::~LPHashTableChained(){
	delete[] slot;
}


///////////////////
// Accessors
///////////////////

// get the element specified by key, if not eligible key return NULL
template<class K, class T, unsigned int containerSize>
T* LPHashTableChained<K,T,containerSize>::search(const K& key){
	Element temp(key,0);
	Element *found = slot[hashFunction(key)].find(temp);
	return (found ? found->data : 0) ;
}

// return the size of the list
template<class K, class T, unsigned int containerSize>
unsigned int LPHashTableChained<K,T,containerSize>::size()const{
	return cSize;
}

// returns true if empty
template<class K, class T, unsigned int containerSize>
bool LPHashTableChained<K,T,containerSize>::empty()const{
	return !cSize;
}


///////////////////
// Modifiers
///////////////////

// insert the unique newData of type T into the slot specified by key of type K
// (if it exists it isn't added again). Returns the address of newData or 0 if 
// function failed to insert the element
template<class K, class T, unsigned int containerSize>
T* LPHashTableChained<K,T,containerSize>::insert(const K& key, const T& newData){
	T *newD;
	// check if the key already exists in the hashTable
	if( (newD = search( key )) )
		return newD;

	try{
		newD = new T(newData);
	}catch(std::bad_alloc&){
		noMemory("LPHashTableChained<K,T,Hash,containerSize>::insert(key,newData)");
	}
		
	Element newE(key,newD);
	if( ! slot[hashFunction(key)].push_front(newE) ){
		return 0;
	}
	cSize++;

	// in order to avoid deallocation of newD we must change the
	// address in the temporary newE element cause the end of this function will
	// call its destructor, thus deleting our newD
	newE.data = 0;

	return newD;
}

// deletes the deleteData of type T specified by the key of type K
// if it does not exist does nothing
template<class K, class T, unsigned int containerSize>
bool LPHashTableChained<K,T,containerSize>::erase(const K& key){
	Element eE(key,0);
	bool r;
	if( (r = ( slot[hashFunction(key)].deleteElement(eE)?true:false ) ) )
		cSize--;
	return r;
}

// deletes all the elements and deallocates their memory
// the slot container remains to have its memory
template<class K, class T, unsigned int containerSize>
void LPHashTableChained<K,T,containerSize>::clear(){
	for( int i=0; i<containerSize; i++ )
		slot[i].clear();
	cSize=0;
}

// returns a vector with all the elements inside in unordered sequence
template<class K, class T, unsigned int containerSize>
LPVector<T> LPHashTableChained<K,T,containerSize>::getElements()const{
	LPVector<T> elements;
	Element* t;
	for( unsigned int i=0; i<containerSize; i++ ){
		/*unsigned int slotSize = slot[i].size();
		for( unsigned int j=1; j<=slotSize; j++ )
			elements.push_back( *(slot[i].getElement(j)->data) );
			*/
		slot[i].rewindNext();
		while( (t = slot[i].getNext()) )
			elements.push_back( *(t->data) );
	}

	return elements;
}

///////////////////
// Operator overloads
///////////////////

// get the element specified by key, if not eligible index return NULL
// calls search()
template<class K, class T, unsigned int containerSize>
T* LPHashTableChained<K,T,containerSize>::operator[](const K& key){
	return search(key);
}

template<class K, class T, unsigned int containerSize>
LPHashTableChained<K,T,containerSize>& LPHashTableChained<K,T,containerSize>::operator=(const LPHashTableChained& other){
	if( this == &other )
		return *this;
	
	// delete old data
	delete[] slot;

	cSize = other.cSize;
	try{
		slot = new LPList<Element>[containerSize];
	}catch(std::bad_alloc&){
		noMemory("LPHashTableChained<K,T,Hash,containerSize>::LPHashTableChained(other)");
	}
	// copy each list from other to this table
	for( unsigned int i=0; i< containerSize; i++ ){
		slot[i] = other.slot[i];
	}
	return *this;
}

#endif
