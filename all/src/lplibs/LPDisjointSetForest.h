/*
 * @author: Lambros Petrou
 * @date: 11/2011
 * @last-modified: 27-11-2011
 * @website: www.lambrospetrou.com
 * @info: 
 *
 */

#ifndef LP_LPDISJOINT_SET_FOREST_H
#define LP_LPDISJOINT_SET_FOREST_H

///////////////////
// MACROS
///////////////////

#ifndef _LP_MEMORY
#define noMemory(x) fprintf(stderr, "Memory Allocation Failed! .::. %s\n", x); exit(1);
#endif

///////////////////
// HEADER INCLUTIONS
///////////////////
#include <cstdlib>
#include <stdio.h>
#include <iostream>
#include <string>
#include <stdexcept>
#include <exception>
#include <iterator>
#include <tr1/unordered_map>

using std::tr1::unordered_map;
using std::tr1::hash;
using std::cout;
using std::cerr;
using std::endl;
using std::string;

//////////////////////////////////////////////////////////////
////////////      DECLERATION OF LPDisjointSetForest  ///////////////////
//////////////////////////////////////////////////////////////

#define MapTToSetPTR std::tr1::unordered_map<T, Set*, hash<T> >

///////////////////
// MAIN CLASS DECLARATIONS
// useful when specializing functions for specific types
///////////////////
template<class T>
class LPDisjointSetForest {
	// PUBLIC
public:
///////////////////
// PUBLIC DECLARATIONS
///////////////////

///////////////////
// Constructors & Destructor
///////////////////
	
	// create a forest with no sets
	LPDisjointSetForest();

	// destructor
	~LPDisjointSetForest();

///////////////////
// Accessors
///////////////////

	// return the size of the forest in elements
	unsigned int elements()const;


///////////////////
// Modifiers
///////////////////

	// create a Set from newValue
	void createSet(const T& newValue);

	// make the set given in arguments a set on its own
	void makeSet( const T& value );

	// unite two sets in the forest
	void uniteSets(const T& a , const T& b);
	
	// returns the root parent of the value in arguments
	// if the value is not in a set in our forest the function prints an error and returns null
	T* findSet( const T& x );

	// returns the number of sets
	unsigned int countSets() const;

	// clear all the elements
	void clear();

///////////////////
// Output
///////////////////

	// prints all the elements with their info
	//void print();

///////////////////
// Operator overloads
///////////////////


	// PRIVATE
private:
	struct Set{
		Set(const T& newData = T() ): data(newData),rank(0),parent(0){}
		Set* parent;
		T data;
		int rank;
	};

	unsigned int elementsS;
	MapTToSetPTR forest;
	//LPHashTableChained< T, Set* > forest;

	unsigned int numberOfSets;

///////////////////
// Private Functions
///////////////////

	// make the set given in arguments a set on its own
	void makeSet( Set& x );
	// link the two sets given in arguments
	void linkSets( Set& a, Set& b );
	// unite two sets in the forest
	void uniteSets(Set &a , Set &b);
	// returns the root parent of the value in arguments
	// if the value is not in a set in our forest the function prints an error and returns null
	Set* findSet( Set& x );

	// not for use by outsiders

	// create an exact replica of other but with different memory
	LPDisjointSetForest(const LPDisjointSetForest& other);
	// delete this sets and create an exact replica of other but with different memory
	LPDisjointSetForest& operator=(const LPDisjointSetForest& other);
};

//////////////////////////////////////////////////////////////
////////////    IMPLEMENTATION OF LPDisjointSetForest  ///////////////
//////////////////////////////////////////////////////////////


///////////////////
// Constructors & Destructor
///////////////////

// create a forest with no sets
template<class T>
LPDisjointSetForest<T>::LPDisjointSetForest(): elementsS(0), numberOfSets(0){

}

/*
// create an exact replica of other but with different memory
template<class T>
LPDisjointSetForest<T>::LPDisjointSetForest(const LPDisjointSetForest& other){
	LPVector<Set*> sa = other.getElements();
	unsigned int sz = sa.size();

	for( unsigned int i=0; i<sz; i++ ){
		Set *s = *( sa[i] );
		if( s ){
			// do here whatever necessary to dublicate the set
		}
	}
}
*/

// destructor
template<class T>
LPDisjointSetForest<T>::~LPDisjointSetForest(){
	clear();
}

///////////////////
// Accessors
///////////////////

// return the size of the forest in elements
template<class T>
unsigned int LPDisjointSetForest<T>::elements()const{
	return elementsS;
}
///////////////////
// Modifiers
///////////////////

// create a Set from newValue
template<class T>
void LPDisjointSetForest<T>::createSet(const T& newValue){
	Set* newSet;
	try{
		newSet= new Set(newValue);
	}catch(std::bad_alloc&){
		noMemory("Error Allocating Memory! .::. LPDisjointSetForest::createSet() ");
		return ;
	}
	newSet->parent = newSet;
	//forest.insert(newValue,newSet);
	forest[newValue] = newSet;
	++elementsS;
	++numberOfSets;
}

// make the set given in arguments a set on its own
template<class T>
void LPDisjointSetForest<T>::makeSet( const T& value ){
	//Set** found = forest.search(value);
	Set** found = forest[value];
	if( !found ){
		cout << "\nThere is no set with the passed in value in Disjoint-Set-Forest!" << endl;
		return 0;
	}
	makeSet( **found );
}

// unite two sets in the forest
template<class T>
void LPDisjointSetForest<T>::uniteSets(const T& a , const T& b){
	//Set **founda = forest.search(a);
	//Set **foundb = forest.search(b);
	Set *founda = forest[a];
	Set *foundb = forest[b];
	if( !founda || !foundb ){
		cout << "\nUnion of two non-sets is not supported!" << endl;
		return;
	}
	linkSets( *founda , *foundb );
}

// returns the root parent of the value in arguments
// if the value is not in a set in our forest the function prints an error and returns null
template<class T>
T* LPDisjointSetForest<T>::findSet( const T& x ){
	//Set** found = forest.search(x);
	Set *found = forest[x];
	if( !found ){
		//cout << "\nThere is no element with the passed in value in Disjoint-Set-Forest!" << endl;
		return 0;
	}
	return &(findSet( *found )->data);
}

// returns the number of sets
template<class T>
unsigned int LPDisjointSetForest<T>::countSets() const{
	return numberOfSets;;
}

// clear all the elements
template<class T>
void LPDisjointSetForest<T>::clear(){
	typename std::tr1::unordered_map<T, Set*, hash<T> >::iterator it = forest.begin();
	typename std::tr1::unordered_map<T, Set*, hash<T> >::iterator end = forest.end();
	for ( ; it != end;	it++) {
		delete (*it).second;
	}
	elementsS = 0;
	forest.clear();
}

///////////////////
// Output
///////////////////
/*
// prints all the elements with their info
template<class T>
void LPDisjointSetForest<T>::print(){
	LPVector<Set*> allSets = forest.getElements();
	unsigned int sz = allSets.size();
	cout << endl;
	for( unsigned int k=0; k<sz; k++ ){
		Set *s=*allSets[k];
		cout << s->data << "\t\t - rank: " << s->rank << " - parent: " << s->parent->data << " - setParent: " << *findSet( s->data ) << endl;
	}
	cout << endl;
}
*/
///////////////////
// Private Functions
///////////////////

// link the two sets given in arguments
template<class T>
void LPDisjointSetForest<T>::linkSets( Set& a, Set& b ){
	if( a.parent == b.parent )
		return;
	if( a.rank > b.rank )
		b.parent = &a;
	else{
		a.parent = &b;
		if( a.rank == b.rank )
			b.rank++;
	}
	numberOfSets--;
}

// returns the root parent of the value in arguments
// if the value is not in a set in our forest the function prints an error and returns null
template<class T>
typename LPDisjointSetForest<T>::Set* LPDisjointSetForest<T>::findSet( Set& x ){
	if( &x != x.parent )
		x.parent = findSet( *x.parent );
	return x.parent;
}

// unite two sets in the forest
template<class T>
void LPDisjointSetForest<T>::uniteSets(Set &a , Set &b){
	linkSets( *findSet(a) , *findSet(b) );
}

// make the set given in arguments a set on its own
template<class T>
void LPDisjointSetForest<T>::makeSet( Set& set ){
	set.parent = &set;
	set.rank = 0;
}

#endif
