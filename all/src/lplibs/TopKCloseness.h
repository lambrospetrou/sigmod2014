/*
 * TopKCloseness.h
 *
 *  Created on: Apr 1, 2014
 *      Author: lambros
 */

#ifndef TOPKCLOSENESS_H_
#define TOPKCLOSENESS_H_

#include <vector>
#include <tr1/unordered_map>



/**
 * http://research.microsoft.com/en-us/people/weic/faw08_centrality.pdf
 */
std::vector<std::pair<long,long> > TopRank2( std::vector<long> &persons,
		std::tr1::unordered_map<long, std::vector<long>, std::tr1::hash<long> > &graph, int k, long N_PERSONS );

#endif /* TOPKCLOSENESS_H_ */
