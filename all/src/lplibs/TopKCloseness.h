/*
 * TopKCloseness.h
 *
 *  Created on: Apr 1, 2014
 *      Author: lambros
 */

#ifndef TOPKCLOSENESS_H_
#define TOPKCLOSENESS_H_

#include <vector>

/**
 * Returns the Top-k closeness centrality nodes.
 *
 * Algorithm 1 - Efficient Top-K Closeness Centrality Search
 *
 */
std::vector<long> top_centrality( std::vector<std::vector<long> > Graph, int k );


#endif /* TOPKCLOSENESS_H_ */
