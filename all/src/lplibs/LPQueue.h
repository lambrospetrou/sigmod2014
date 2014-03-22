#ifndef LP_QUEUE_H
#define LP_QUEUE_H

#include <exception>
#include <iostream>
#include <string>

using std::cout;
using std::cerr;
using std::endl;
using std::exception;
using std::bad_alloc;
using std::string;

//////////////////////////////////////////////////////////////
/////////      DECLERATION OF LPQueue<>     //////////////////
//////////////////////////////////////////////////////////////
template<class T>
class LPQueue{
	// PUBLIC
public:
	// Constructors & Destructor
	LPQueue();
	LPQueue(const T& newData);
	LPQueue(const LPQueue & other);
	~LPQueue();

	// Accessors
	bool empty()const;
	size_t size()const;
	T* front();
	T* back();
	const T* front()const;
	const T* back()const;

	// Modifiers
	void pop();
	void push(const T& newData);
	void clear();

	// Operator overloads
	LPQueue& operator=(const LPQueue & other);

	// PRIVATE
private:
	struct Node{
		T data;
		Node *next;
		Node(T d):data(d),next(0){}
	};

	size_t currentSize;
	Node* frontN;
	Node* backN;
};


//////////////////////////////////////////////////////////////
//////      IMPLEMENTATION OF LPQueue<>      /////////////////
//////////////////////////////////////////////////////////////

///////////////////
// Constructors & Destructor
///////////////////
template<class T>
LPQueue<T>::LPQueue(void):currentSize(0),frontN(0),backN(0){

}

template<class T>
LPQueue<T>::LPQueue(const T& newData){
	Node *newNode = new Node(newData);
	currentSize=1;
	frontN = backN = newNode;
}

template<class T>
LPQueue<T>::LPQueue(const LPQueue & other){
	currentSize = other.currentSize;
	if( 0 < currentSize ){
		try{
			frontN = new Node(other.frontN->data);
		}catch(bad_alloc&){
			cerr << "Memory Allocation failed!" << endl;
			return;
		}

		// copy each one of other's elements into this
		Node* curOther = other.frontN;
		Node* cur = frontN;
		while( curOther->next ){
			curOther = curOther->next;
			try{
				cur->next = new Node(curOther->data);
			}catch(bad_alloc&){
				cerr << "Memory Allocation failed!" << endl;
				return;
			}
			cur = cur->next;
		}
		backN = cur;
	}else{
		frontN = backN = 0;
	}
}

template<class T>
LPQueue<T>::~LPQueue(){
	for(Node *t; frontN; frontN=t ){
		t=frontN->next;
		delete frontN;
	}
}

///////////////////
// Accessors
///////////////////
template<class T>
bool LPQueue<T>::empty()const{
	return currentSize?false:true;
}

template<class T>
unsigned int LPQueue<T>::size()const{
	return currentSize;
}

template<class T>
T* LPQueue<T>::front(){
	return &frontN->data;
}

template<class T>
const T* LPQueue<T>::front()const{
	return &frontN->data;
}

template<class T>
T* LPQueue<T>::back(){
	return &backN->data;
}

template<class T>
const T* LPQueue<T>::back()const{
	return &backN->data;
}

///////////////////
// Modifiers
///////////////////
template<class T>
void LPQueue<T>::pop(){
	if( 0 == currentSize )   // no element to remove
		return;
	Node *del = frontN;
	frontN = frontN->next;
	try{
		delete del;
	}catch(exception& e){
		cerr << "Exception thrown: " << e.what() << endl;
		return;
	}
	currentSize--;
	if( 0 == currentSize)
		backN = 0;
}

template<class T>
void LPQueue<T>::push(const T& newData){
	Node *newNode;
	try{
		newNode = new Node(newData);
	}catch(bad_alloc&){
		cerr << "Memory allocation failed!" <<endl;
		return;
	}

	if( empty() ){
		backN = frontN = newNode;
	}else{
		backN->next = newNode;
		backN = newNode;
	}
	currentSize++;
}

template<class T>
void LPQueue<T>::clear(){
	if( empty() )
		return;
	for( Node *t; frontN; frontN=t ){
		t = frontN->next;
		try{
			delete frontN;
		}catch(exception& e){
			cerr << "Exception thrown: " << e.what() << endl;
			return;
		}
	}
	currentSize=0;
	frontN = backN = 0;
}

///////////////////
// Operator overloads
///////////////////
template<class T>
LPQueue<T>& LPQueue<T>::operator=(const LPQueue & other){
	if( this == &other )
		return *this;
	// De-Allocate our data first
	clear();

	// check if the other queue has any elements and copy them
	if( 0 < other.currentSize ){
		try{
			frontN = new Node(other.frontN->data);
		}catch(bad_alloc&){
			cerr << "Memory Allocation failed!" << endl;
			return *this;
		}

		// copy each one of other's elements into this
		Node *curOther = other.frontN;
		Node *cur = frontN;
		while( curOther->next ){
			curOther = curOther->next;
			try{
				cur->next = new Node(curOther->data);
			}catch(bad_alloc&){
				cerr << "Memory Allocation failed!" << endl;
				return *this;
			}
			cur = cur->next;
		}
		backN = cur;
		currentSize = other.currentSize;
	}

	return *this;
}







#endif