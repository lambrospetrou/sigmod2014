/*
 * TopKCloseness.cpp
 *
 *  Created on: Apr 1, 2014
 *      Author: lambros
 */

#include "TopKCloseness.h"

#include <vector>
#include <tr1/unordered_map>

using std::vector;
using std::tr1::unordered_map;
using std::tr1::hash;

/**
 * Returns the Top-k closeness centrality nodes.
 *
 * Algorithm 1 - Efficient Top-K Closeness Centrality Search
 *
 *	input: graph G, top K number
 *	output: top-k list of most central nodes
 *
 */
vector<long> top_centrality( std::vector<std::vector<long> > Graph, int k ){

	return vector<long>();
}

/**
 * Returns the Top-k closeness centrality nodes.
 *
 * Algorithm 2 - Efficient Top-K Closeness Centrality Search
 *
 *	input: 	vertex p
 *			vertex-level map L
 *			sum of geodesic distances s
 *			top-k list A
 *			schedule S
 *			top K
 *
 */
void process(long p, unordered_map<long, long, hash<long> > L, long s, vector<long> A, vector<long> S, int k){

}



