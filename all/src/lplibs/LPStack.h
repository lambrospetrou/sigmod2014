#ifndef LP_STACK_H
#define LP_STACK_H

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
/////////      DECLERATION OF LPStack<>     //////////////////
//////////////////////////////////////////////////////////////
template<class T>
class LPStack{
	// PUBLIC
public:
	// Constructors & Destructor
	LPStack();
	LPStack(const T& newData);
	LPStack(const LPStack & other);
	~LPStack();

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
	LPStack& operator=(const LPStack & other);

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
//////      IMPLEMENTATION OF LPStack<>      /////////////////
//////////////////////////////////////////////////////////////

///////////////////
// Constructors & Destructor
///////////////////
template<class T>
LPStack<T>::LPStack(void):currentSize(0),frontN(0),backN(0){

}

template<class T>
LPStack<T>::LPStack(const T& newData){
	Node *newNode = new Node(newData);
	currentSize=1;
	frontN = backN = newNode;
}

template<class T>
LPStack<T>::LPStack(const LPStack & other){
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
LPStack<T>::~LPStack(){
	for(Node *t; frontN; frontN=t ){
		t=frontN->next;
		delete frontN;
	}
}

///////////////////
// Accessors
///////////////////
template<class T>
bool LPStack<T>::empty()const{
	return currentSize?false:true;
}

template<class T>
unsigned int LPStack<T>::size()const{
	return currentSize;
}

template<class T>
T* LPStack<T>::front(){
	return &frontN->data;
}

template<class T>
const T* LPStack<T>::front()const{
	return &frontN->data;
}

template<class T>
T* LPStack<T>::back(){
	return &backN->data;
}

template<class T>
const T* LPStack<T>::back()const{
	return &backN->data;
}

///////////////////
// Modifiers
///////////////////
template<class T>
void LPStack<T>::pop(){
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
void LPStack<T>::push(const T& newData){
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
		newNode->next = frontN;
		frontN = newNode;
	}
	currentSize++;
}

template<class T>
void LPStack<T>::clear(){
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
LPStack<T>& LPStack<T>::operator=(const LPStack & other){
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