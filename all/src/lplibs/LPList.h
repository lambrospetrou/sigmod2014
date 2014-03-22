/*
 * @author: Lambros Petrou
 * @date: 11/2011
 * @last-modified: 25-11-2011
 * @website: www.lambrospetrou.com
 * @info: LPList is a container that use dynamic memory for its storage
 *        and it uses nodes to hold each element data. it is very efficient
 *        for insertion and deletion at any position and also for linear ietration
 *        not for efficient for direct element access by index
 *
 */

#ifndef LP_LIST_H
#define LP_LIST_H

///////////////////
// MACROS
///////////////////

// called when memory allocation fails, prints an error message and exits
#define noMemory(x) cerr << "Memory Allocation Failed! .::. "##x << endl; exit(1);

///////////////////
// HEADER INCLUTIONS
///////////////////
#include <cstdlib>
#include <iostream>
#include <string>
#include <stdexcept>
#include <exception>
#include <new>

using std::cerr;
using std::endl;
using std::string;

//////////////////////////////////////////////////////////////
////////////      DECLERATION OF LPList   ///////////////////
//////////////////////////////////////////////////////////////
template<class T>
class LPList{
	// PUBLIC
public:
///////////////////
// PUBLIC DECLARATIONS
///////////////////


///////////////////
// Constructors & Destructor
///////////////////
	
	// default constructor
	LPList();

	// copy constructor
	LPList(const LPList& other);

	// destructor
	virtual ~LPList();

///////////////////
// Accessors
///////////////////

	// get the element at the front of the list
	T* front();

	// get the element at the back of the list
	T* back();

	// return the size of the list
	unsigned int size()const;

	// get the element specified by index parameter, if not eligible index return NULL
	// elements are numbered from 1 - size()
	T* getElement(unsigned int index);

	// get the first element that its data matches the lookUpData, NULL if does not exist
	T* find(const T& lookUpData);

	// this function returns a pointer to the next data after your last call to it
	// when all elements are returned it returns 0
	// if you recall it after returning 0 it will start from the beginning
	T* getNext();

	// resets the next element that the function above returns to the first element
	void rewindNext();

///////////////////
// Modifiers
///////////////////

	// insert the new data at the front of the list
	bool push_front(const T& newData);

	// deletes the element at the front of the list
	bool pop_front();

	// insert the new data at the back of the list
	bool push_back(const T& newData);

	// deletes the element at the back of the list
	bool pop_back();

	// insert the newData after the position specified by index, elements are numbered from 1 - size()
	// if not eligible index is given the newData is inserted at the back of the list
	bool insertElement(const T& newData, unsigned int index);

	// deletes the element specified by index, if not eligible index does nothing
	// elements are numbered from 1 - size()
	bool deleteAt(unsigned int index);

	// deletes the first element that its data matches the deleteData
	unsigned int deleteElement(const T& deleteData);

	// deletes all the elements that their data matches the deleteData
	// returns the number of deleted elements
	unsigned int deleteAllElements(const T& deleteData);

	// clears all the elements from the list and deallocates their space
	void clear();

///////////////////
// Output
///////////////////


///////////////////
// Operator overloads
///////////////////

	// get the element specified by index, if not eligible index return NULL
	// elements are numbered from 1 - size()
	// calls getElement()
	T* operator[](unsigned int index);

	// delete the current data and copy from other into this with its own memory
	LPList<T>& operator=(const LPList& other);

	// PRIVATE
private:
	struct Node{
	public:
		T data;
		Node *next;
		Node *prev;
		Node(const T& nData):data(nData),next(0),prev(0){}
	};

	Node *frontN;
	Node *backN;
	Node *next;
	unsigned int cSize;

	// private functions

};

//////////////////////////////////////////////////////////////
////////////      IMPLEMENTATION OF LPList   ///////////////////
//////////////////////////////////////////////////////////////

///////////////////
// Constructors & Destructor
///////////////////

// default constructor
template<class T>
LPList<T>::LPList():frontN(0),backN(0),cSize(0),next(0){}

// copy constructor
template<class T>
LPList<T>::LPList(const LPList& other):frontN(0),backN(0),cSize(0),next(0){
	Node* cNode = other.frontN;
	for( ; cNode ; cNode=cNode->next )
		push_front(cNode->data);
}

// destructor
template<class T>
LPList<T>::~LPList(){
	clear();
}

///////////////////
// Accessors
///////////////////

// get the element at the front of the list
template<class T>
T* LPList<T>::front(){
	return ( frontN ? &frontN->data : 0 );
}

// get the element at the back of the list
template<class T>
T* LPList<T>::back(){
	return ( backN ? &backN->data : 0 );
}

// return the size of the list
template<class T>
unsigned int LPList<T>::size()const{
	return cSize;
}

// get the element specified by index parameter, if not eligible index return NULL
// elements are numbered from 1 - size()
template<class T>
T* LPList<T>::getElement(unsigned int index){
	if( index > cSize ){
		cerr << "Out-of-Range Index! :: LPList<T>::getElement()" << endl;
		return 0;
	}
	Node *t = frontN;
	for( ; index > 1 ; index--,t=t->next );
	return ( t ? &t->data : 0 );
}

// get the first element that its data matches the lookUpData, NULL if does not exist
// elements of type T must have the function operators == and != overloaded
template<class T>
T* LPList<T>::find(const T& lookUpData){
	Node *t = frontN;
	for( ; t && (t->data != lookUpData) ; t=t->next );
	return ( t ? &t->data : 0 );
}

// this function returns a pointer to the next data after your last call to it
// when all elements are returned it returns 0
// if you recall it after returning 0 it will start from the beginning
template<class T>
T* LPList<T>::getNext(){
	if( !next ){
		// we must start over again
		next = frontN;
	}else{
		// we must move to the next element
		next = next->next;
	}
	// return the data of the element
	return ( next ? &next->data : 0 );
}

// resets the next element that the function above returns to the first element
template<class T>
void LPList<T>::rewindNext(){
	next = 0;
}

///////////////////
// Modifiers
///////////////////

// insert the new data at the front of the list
template<class T>
bool LPList<T>::push_front(const T& newData){
	Node *t; 
	try{
		t = new Node(newData);
	}catch(std::bad_alloc&){
		noMemory("LPList<T>::push_front()");
		return false;
	}
	
	t->next = frontN;
	if( frontN )
		frontN->prev = t;
	frontN = t;
	if( !backN )
		backN = t;
	cSize++;
	return true;
}

// deletes the element at the front of the list
template<class T>
bool LPList<T>::pop_front(){
	if( !frontN ) return false;
	Node *t = frontN;
	frontN = frontN->next;
	if( frontN )
		frontN->prev = 0;
	delete t;
	cSize--;
	// if the list has no remaining elements fix the backN pointer
	if( !frontN ) 
		backN = 0;
	return true;
}

// insert the new data at the back of the list
template<class T>
bool LPList<T>::push_back(const T& newData){
	Node *t;
	try{
		t = new Node(newData);
	}catch(std::bad_alloc&){
		noMemory("LPList<T>::push_back()");
		return false;
	}
	
	if( backN )
		backN->next = t;
	t->prev = backN;
	backN = t;
	if( !frontN )
		frontN = t;
	cSize++;
	return true;
}

// deletes the element at the back of the list
template<class T>
bool LPList<T>::pop_back(){
	if( !backN ) return false;
	Node *t = backN;
	backN = backN->prev;
	if( backN )
		backN->next = 0;
	delete t;
	cSize--;
	if( !backN )
		frontN = 0;
	return true;
}

// insert the newData after the position specified by index, elements are numbered from 1 - size()
// if not eligible index is given the newData is inserted at the back of the list
template<class T>
bool LPList<T>::insertElement(const T& newData, unsigned int index){
	if( index > cSize )
		return push_back(newData);
	else if( index==0 )
		return push_front(newData);
	else{
		Node *t,*newN;
		try{
			newN = new Node(newData);
		}catch(std::bad_alloc&){
			noMemory("LPList<T>::insertElement()");
			return false;
		}
		if( index > cSize/2 ){
			// we will start iterating from the back of the list for faster insertion
			t = backN;
			index = cSize - index;
			for( ; index > 0; t=t->prev, index-- );
		}else{
			// we will start iterating from the front of the list for faster insertion
			t = frontN;
			for( ; index > 1; t=t->next, index-- );
		}
		newN->next = t->next;
		if( t->next )
			t->next->prev = newN;
		newN->prev = t;
		t->next = newN;
		// check if newNode is the new back element in the list
		if( !newN->next )
			backN = newN;
		cSize++;
		return true;
	}
}

// deletes the element specified by index, if not eligible index does nothing
// elements are numbered from 1 - size()
template<class T>
bool LPList<T>::deleteAt(unsigned int index){
	if( !frontN || index>cSize )
		return false;
	Node *t;
	if( index > cSize/2 ){
		// we will start iterating from the back of the list for faster insertion
		t = backN;
		index = cSize - index;
		for( ; index > 0; t=t->prev, index-- );
	}else{
		// we will start iterating from the front of the list for faster insertion
		t = frontN;
		for( ; index > 1; t=t->next, index-- );
	}
	if( t == frontN )
		frontN = frontN->next;
	if( t == backN )
		backN = backN->prev;
	if( t->prev )
		t->prev->next = t->next;
	if( t->next )
		t->next->prev = t->prev;
	delete t;
	cSize--;
	return true;
}

// deletes the first element that its data matches the deleteData
template<class T>
unsigned int LPList<T>::deleteElement(const T& deleteData){
	if( !frontN ) return 0;

	Node *t = frontN;
	for( ; t ; t=t->next ){
		if( t->data == deleteData ){
			if( t == frontN )
				frontN = frontN->next;
			if( t == backN )
				backN = backN->prev;
			if( t->prev )
				t->prev->next = t->next;
			if( t->next )
				t->next->prev = t->prev;
			delete t;
			cSize--;
			return 1;
		}
	}
	return 0;
}

// deletes all the elements that their data matches the deleteData
// returns the number of deleted elements
template<class T>
unsigned int LPList<T>::deleteAllElements(const T& deleteData){
	if( !frontN ) return 0;

	Node *t = frontN, *next;
	unsigned int c=0;
	for( ; t ; t=next ){
		next = t->next;
		if( t->data == deleteData ){
			if( t == frontN )
				frontN = frontN->next;
			if( t == backN )
				backN = backN->prev;
			if( t->prev )
				t->prev->next = t->next;
			if( t->next )
				t->next->prev = t->prev;
			delete t;
			cSize--;
			c++;
		}
	}
	return c;
}

// clears all the elements from the list and deallocates their space
template<class T>
void LPList<T>::clear(){
	Node *cur = frontN, *t;
	for( ; cur ; cur=t ){
		t = cur->next;
		delete cur;
	}

	frontN = 0;
	backN = 0;
	cSize = 0;
}

///////////////////
// Output
///////////////////


///////////////////
// Operator overloads
///////////////////

// get the element specified by index, if not eligible index return NULL
// elements are numbered from 1 - size()
// calls getElement()
template<class T>
T* LPList<T>::operator[](unsigned int index){
	return getElement(index);
}


// delete the current data and copy from other into this with its own memory
template<class T>
LPList<T>& LPList<T>::operator=(const LPList& other){
	if( this == &other )
		return *this;
	
	// delete all current elements
	clear();
	// copy the elements from other
	Node* cNode = other.frontN;
	for( ; cNode ; cNode=cNode->next )
		push_front(cNode->data);
	return *this;
}


///////////////////
// PRIVATE FUNCTIONS
///////////////////

#endif