/*
 * @author: Lambros Petrou
 * @date: 10/2011
 * @last-modified: 10-10-2011
 * @website: www.lambrospetrou.com
 * @info: LPVector is a container that uses a normal array as its base,
 *		  dynamically allocated when data is pushed and auto inflated  
 *        when more data than maximum need to be inserted
 *
 */

#ifndef LP_VECTOR_H
#define LP_VECTOR_H

///////////////////
// MACROS
///////////////////

// called when memory allocation fails, prints an error message and exits
#ifndef _LP_MEMORY
#define noMemory(x) fprintf(stderr, "Memory Allocation Failed! .::. %s\n", x); exit(1);
#endif

///////////////////
// HEADER INCLUTIONS
///////////////////
#include <exception>
#include <stdexcept>
#include <iostream>
#include <string>
#include <cstring>
#include <cstdlib>

using std::cout;
using std::cerr;
using std::endl;
using std::exception;
using std::bad_alloc;
using std::out_of_range;
using std::string;

//////////////////////////////////////////////////////////////
/////////      DECLERATION OF LPVector<>    //////////////////
//////////////////////////////////////////////////////////////
template<class T>
class LPVector{
	// PUBLIC
public:
///////////////////
// Constructors & Destructor
///////////////////

	// default constructor
	LPVector();

	// creates a container with n elements holding the value of newData
	LPVector(unsigned int n, const T& newData);

	// create a container by another container
	LPVector(const LPVector & other);

	// destructor
	virtual ~LPVector();

///////////////////
// Accessors
///////////////////

	// returns the size of container
	unsigned int size()const;

	// returns true if container is empty
	bool empty()const;

	// returns pointer to the front entry at index 0
	T* front();

	// returns pointer to the back entry, index=size-1, next to be pop()
	T* back();

///////////////////
// Modifiers
///////////////////

	// inserts the newData at the back
	void push_back(const T& newData);

	// removes and deallocates the last element
	void pop_back();

	// resize the container's size to newSize, if larger than current push as many 
	// elements as newSize-size and if smaller just de-allocate excessive elements
	void resize(unsigned int newSize, T c=T() );

	// clears all content of container effectively de-allocating the memory reserved
	// all members get their default values for the class
	void clear();

	// sorts the elements using function comp() which returns true if the 
	// first argument goes before second argument when return value is true
	void sort(bool (*comp)(const T& a, const T& b) = myLessThan);

///////////////////
// Operator overloads
///////////////////

	// makes THIS container an exact replica of other but with its own space in memory
	LPVector& operator=(const LPVector & other);

	// returns a pointer to the element pointed by index
	T* operator[](unsigned int index);

	// PRIVATE
private:
	enum{DEFAULT_INFLATE=100};
	unsigned int currentSize;
	unsigned int maxSize;
	T** elements;

///////////////////
// Private Functions
///////////////////

	// increases the maximum size of container by allocating more space
	// copying the old data into the new space and deleting the old space
	bool inflate(unsigned int increase);

	// the default function for comparisons in sort
	// assumes the operator < is defined for T
	bool myLessThan( const T& a, const T& b );

	// sorting algorithms
	void insertionSort(unsigned int start, unsigned int end, bool (*comp)(const T& a, const T& b));

	void mergeSort(unsigned int start, unsigned int end, bool (*comp)(const T& a, const T& b));
	void merge(unsigned int start, unsigned int mid, unsigned int end, bool (*comp)(const T& a, const T& b));

};



//////////////////////////////////////////////////////////////
/////////   IMPLEMENTATION OF LPVector<>    //////////////////
//////////////////////////////////////////////////////////////

///////////////////
// Constructors & Destructor
///////////////////

// default constructor
template<class T>
LPVector<T>::LPVector():currentSize(0),maxSize(0),elements(0){

}


// constructor creates a container with 'n' elements with value of 'newData'
template<class T>
LPVector<T>::LPVector(unsigned int n, const T& newData){
	try{
		elements = new T*[n];
		for(unsigned int i=0; i<n; i++)
			elements[i]	= new T(newData);
	}catch(bad_alloc&){
		noMemory("LPVector<T>::LPVector(n,newData)");
		return;
	}
	currentSize	= n;
	maxSize = n;
}

// copy constructor just copies all elements from other to THIS 
// including container data	with different memory space
template<class T>
LPVector<T>::LPVector(const LPVector & other){
	// initialize privates of this
	elements = 0;
	currentSize = 0;
	maxSize = other.maxSize;

	if( 0 == maxSize ){
		return;
	}

	if( !inflate( maxSize )){
		noMemory("push_back() -> inflate()");
		return;
	}
	for( unsigned int i=0; i<other.currentSize; i++ )
		push_back(*(other.elements[i]));
}

// destructor de-allocates any allocated space
template<class T>
LPVector<T>::~LPVector(){
	for(unsigned int i=0; i<currentSize; ++i )
		delete elements[i];
	delete[] elements;
}

///////////////////
// Accessors
///////////////////

// returns container's size so far
template<class T>
unsigned int LPVector<T>::size()const{
	return currentSize;
}

// returns true if container empty
template<class T>
bool LPVector<T>::empty()const{
	return currentSize?false:true;
}

// returns a pointer to the data at the back of the container
template<class T>
T* LPVector<T>::front(){
	return (currentSize==0)?0:elements[0];
}

// returns a pointer to the data at the back of the container, next to be poped()
template<class T>
T* LPVector<T>::back(){
	return (currentSize==0)?0:elements[currentSize-1];
}

///////////////////
// Modifiers
///////////////////

// allocates space for the newData and inserts it at the back. If container is full then we inflate
// the container to hold more data by calling inflate() method
template<class T>
void LPVector<T>::push_back(const T& newData){
	if( currentSize == maxSize ){
		if( !inflate( (0==maxSize)?DEFAULT_INFLATE:maxSize )){
			if( !inflate(DEFAULT_INFLATE) ){
				noMemory("Memory allocation failed! .::. LPVector::push_back() -> inflate()");
				return;
			}
		}
	}
	
	try{
		elements[currentSize] = new T(newData);
	}catch(bad_alloc&){
		noMemory("Memory allocation failed! .::. LPVector::push_back()");
		return;
	}
	currentSize++;
}

// de-allocates the last element of the container reducing its size by 1
template<class T>
void LPVector<T>::pop_back(){
	if( 0 == currentSize )
		return;
	delete elements[currentSize-1];
	--currentSize;
	elements[currentSize] = 0;
}

// resize the container's size to newSize, if larger than current push as many 
// elements as newSize-size and if smaller just de-allocate excessive elements
template<class T>
void LPVector<T>::resize(unsigned int newSize, T c ){
	if( newSize < currentSize ){
		// we must remove the elements in excess
		for( --currentSize; currentSize>=newSize; --currentSize ){
			delete elements[currentSize];
			elements[currentSize] = 0;
		}
	}else{
		// we must insert more elements (c) to meet newSize
		while( currentSize < newSize )
			push_back(c);
	}
}

// clears all content of container effectively de-allocating the memory reserved
// all members get their default values for the class
template<class T>
void LPVector<T>::clear(){
	if( 0 == maxSize )
		return;
	for(unsigned int i=0; i<currentSize; ++i )
		delete elements[i];
	delete[] elements;
	elements = 0;
	currentSize = maxSize = 0;
}

// sorts the elements using function comp() which returns true if the 
// first argument goes before second argument when return value is true
template<class T>
void LPVector<T>::sort(bool (*comp)(const T& a, const T& b)){
	//insertionSort(0,currentSize,comp);
	if( currentSize )
		mergeSort( 0 , currentSize-1 , comp );
}

// start = the first element index
// end = last element index + 1
// comp = function defining if xI must be swapped with xJ where I<J
template<class T>
void LPVector<T>::insertionSort(unsigned int start, unsigned int end, bool (*comp)(const T& a, const T& b)){
	// Insertion Sort
	unsigned int j;
	for( unsigned int i=start+1; i<end; i++ ){
		T *t = elements[i];
		for( j=i; j>0; j-- ){
			if( comp( *t , *(elements[j-1]) ) )
				elements[j] = elements[j-1];
			else
				break;
		}
		elements[j] = t;
	}
}

// start = the first element index
// end = last element index
// comp = function defining if xI must be swapped with xJ where I<J
template<class T>
void LPVector<T>::mergeSort(unsigned int start, unsigned int end, bool (*comp)(const T& a, const T& b)){
	// Merge Sort
	if( start < end ){
		unsigned int mid = start + (( end-start ) / 2 );
		mergeSort( start, mid, comp );
		mergeSort( mid + 1, end , comp );
		merge( start, mid, end, comp );
	}
}

template<class T>
void LPVector<T>::merge(unsigned int start, unsigned int mid, unsigned int end, bool (*comp)(const T& a, const T& b)){
	unsigned int n1 = mid - start + 1;
	unsigned int n2 = end - mid;
	T** left;
	T** right;
	try{
		left = new T*[n1];
		right = new T*[n2];
	}catch(std::bad_alloc&){
		noMemory("Memory Allocation Failed! LPVector<T>::mergeSort()");
	}

	for( unsigned int i=0; i <n1; i++ )
		left[i] = elements[start+i];
	for( unsigned int i=0; i <n2; i++ )
		right[i] = elements[mid+i+1];

	unsigned int i=0,j=0;
	for( unsigned int k = start; k<=end; k++ ){
		if( i<n1 && j>=n2 ){
			// only left has elements
			elements[k] = left[i];
			i++;
		}else if( i>=n1 && j<n2 ){
			// only right has elements
			elements[k] = right[j];
			j++;
		}else if( comp( *(left[i]), *(right[j]) ) ){
			// left element should be before right element
			elements[k] = left[i];
			i++;
		}else{
			// right element should be before left element
			elements[k] = right[j];
			j++;
		}
	}
	delete[] left;
	delete[] right;
}


///////////////////
// Operator overloads
///////////////////

// makes THIS container exactly the same with other container
// it clears the current content and pushes all the elements by other container
template<class T>
LPVector<T>& LPVector<T>::operator=(const LPVector & other){
	if( this == &other )
		return *this;
	// clear any existing content
	clear();

	maxSize = other.maxSize;
	if( !inflate( maxSize )){
		//cerr << "Memory allocation failed! :: operator=() -> inflate()" << endl;
		noMemory("operator=() -> inflate()");
		return *this;
	}
	for( unsigned int i=0; i<other.currentSize; i++ )
		push_back(*(other.elements[i]));

	return *this;
}

// returns the element indicated by index or NULL if out of range
template<class T>
T* LPVector<T>::operator[](unsigned int index){
	if( index <0 || index > currentSize )
		throw out_of_range("Out-Of-Range index .::. LPVector::operator[]");
	return elements[index];
}

///////////////////
// Private functions
///////////////////

// increase the amount of memory allocated to "elements" by "increase" argument
// it copies all elements to the new position and deallocates the old ones
// CAUTION: LPVector hold pointers to its data so it does not deallocate their memory
//          just the elements memory. To free up the whole space one should use clear()
// if it fails to find new memory space as large it returns the elements address unaffected
template<class T>
bool LPVector<T>::inflate(unsigned int increase){
	if( 0 == increase )
		return true;
	// initialize privates of this 
	T** newElements;
	unsigned int szT = sizeof(T*);
	try{
		newElements = new T*[maxSize + increase];
	}catch(bad_alloc& e){
		//cerr << "Memory Allocation failed!" << endl;
		noMemory("inflate()");
		throw e;
		return false;
	}
	if( elements != 0){
		for(unsigned int i=0; i<maxSize; i++)
			newElements[i] = elements[i];
		delete[] elements;
	}
	maxSize = maxSize + increase;
	elements  = newElements;
	return true;
}



// the default function for comparisons in sort
// assumes the operator < is defined for T
template<class T>
bool LPVector<T>::myLessThan( const T& a, const T& b ){
	return a<b;
}

#endif
