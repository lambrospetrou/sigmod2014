/*
 * TopKCloseness.cpp
 *
 *  Created on: Apr 1, 2014
 *      Author: lambros
 */

#include "TopKCloseness.h"

#include <stdlib.h>
#include <limits.h>
#include <string.h>
#include <math.h>
#include <stdio.h>

#include <vector>
#include <tr1/unordered_map>
#include <algorithm>

using std::vector;
using std::tr1::unordered_map;
using std::tr1::hash;

typedef std::tr1::unordered_map<long, std::vector<long>, std::tr1::hash<long> > MAP_LONG_VecL;

struct EstimationNode{
	EstimationNode(){
		personId = -1;
		distance = -1;
	}
	EstimationNode(long id, long d){
		personId = id;
		distance = d;
	}
	long personId;
	long distance;
};

bool EstimationNodeInc(const EstimationNode &a, const EstimationNode &b){
	if( a.distance == b.distance )
		return a.personId <= b.personId;
	return a.distance <= b.distance;
}

bool EstimationNodeDesc(const EstimationNode &a, const EstimationNode &b){
	if( a.distance == b.distance )
		return a.personId <= b.personId;
	return a.distance >= b.distance;
}


long calculateGeodesicDistance( MAP_LONG_VecL &newGraph, long cPerson, char* visited, long *GeodesicBFSQueue, long N_PERSONS){
	//fprintf(stderr, "c[%d-%d] ", cPerson, localMaximumGeodesicDistance);
	long gd=0;
	memset(visited, -1, N_PERSONS);
	long qIndex = 0;
	long qSize = 1;
	GeodesicBFSQueue[0] = cPerson;
	visited[cPerson] = 0;
	long depth;
	long cAdjacent;
	while(qIndex<qSize){
		cPerson = GeodesicBFSQueue[qIndex];
		depth = visited[cPerson];
		gd += depth;
		vector<long> &edges = newGraph[cPerson];
		for( long e=0,esz=edges.size(); e<esz; e++ ){
			cAdjacent = edges[e];
			if( visited[cAdjacent] >= 0 )
				continue;
			visited[cAdjacent] = depth+1;
			GeodesicBFSQueue[qSize++] = cAdjacent;
		}
		// next vertex from queue
		qIndex++;
	}
	return gd;
}

double F_l( long N_PERSONS, int samples_l ){
	int a_ = 3; // constant number > 1
	return a_ * sqrt(log2(N_PERSONS)/(samples_l*1.0));
	//return 1;
}

/**
 * http://jgaa.info/accepted/2004/EppsteinWang2004.8.1.pdf
 */
void Rand( std::vector<long> &persons, MAP_LONG_VecL &graph, int k, long N_PERSONS,
		int samples, long *PersonCentralityEstimations, char*visited, long*Q, long *minMaxG,
		long startRandom, long endRandom, std::vector<EstimationNode>& result){

	long qIndex, qSize, cAdjacent, depth;

	char minMax=100, currentMax;

	// TODO - CHOOSE PEOPLE IN RANDOM - NOW I CHOOSE THEM AS THEY APPEARED IN THE
	// FORUM HAS MEMBER FILE - persons order
	for( long i = startRandom, end=endRandom; i<end; i++ ){
		long cPerson = persons[i];
		memset(visited, -1, N_PERSONS);
		// do BFS for this sample and update all the distances
		currentMax=0;
		qIndex = 0;
		qSize = 1;
		Q[qIndex] = cPerson;
		visited[cPerson] = 0;
		while( qIndex < qSize ){
			cPerson = Q[qIndex++];
			// update the centrality estimation distance
			PersonCentralityEstimations[cPerson] += visited[cPerson];

			if( currentMax < visited[cPerson] )
				currentMax = visited[cPerson];

			depth = visited[cPerson] + 1;
			std::vector<long> &edges = graph[cPerson];

			if( edges.size() == 0 ){
				fprintf(stderr, "no edges [%ld]\n", cPerson);
			}

			for( long e=0,esz=edges.size(); e<esz; e++ ){
				cAdjacent = edges[e];
				if( visited[cAdjacent] == -1 ){
					visited[cAdjacent] = depth;
					Q[qSize++] = cAdjacent;
				}
			}
		}
		//fprintf(stderr, "minMax[%d] currentMax[%d]\n", minMax, currentMax);
		if( minMax > currentMax )
			minMax = currentMax;
	}// end of sampling

	*minMaxG = minMax;

	result.clear();
	for( long i=0; i<N_PERSONS; i++ ){
		if( PersonCentralityEstimations[i] == 0 )
			continue;
		result.push_back(EstimationNode(i, PersonCentralityEstimations[i]));
	}
}

/**
 * http://research.microsoft.com/en-us/people/weic/faw08_centrality.pdf
 */
vector<std::pair<long,long> > TopRank2( vector<long> &persons, MAP_LONG_VecL &graph, int k, long N_PERSONS ){

	long *PersonCentralityEstimations = (long*)malloc(sizeof(long)*N_PERSONS);
	memset(PersonCentralityEstimations, 0, sizeof(long)*N_PERSONS);
	char *visited = (char*)malloc(N_PERSONS);
	long *Q = (long*)malloc(N_PERSONS*sizeof(long));

	// here we have to run the RAND algorithm
	// choose a value for the initial samples
	// I choose it to be 4*k since k is small in our cases
	int samples = k<<6;

	// STEP 1
	long minMax;
	vector<EstimationNode> result;
	Rand(persons, graph, k, N_PERSONS, samples,PersonCentralityEstimations,
			visited, Q, &minMax, 0, samples, result);
	std::stable_sort(result.begin(), result.end(), EstimationNodeInc);

	// STEP 2
	double f_l = F_l(N_PERSONS,samples);
	long D = 2 * minMax;

	// STEP 3
	double threshold = (result.at(k).distance / (1.0*samples)) + 2*f_l*D;

	// STEP 4
	char *currentE = (char*)malloc(N_PERSONS);
	memset(currentE, 0, N_PERSONS);
	long p, p_, q=(long)log2(N_PERSONS);
	char *temp;

	// calculate p
	p_ = 0;
	/*
	for (long i = 0; i < N_PERSONS; i++) {
		if ( PersonCentralityEstimations[i] > 0 && (PersonCentralityEstimations[i] / 1.0*samples) <= threshold) {
			previousE[i] = 1;
			p_++;
		}
	}
	*/
	for( long i=0,sz=result.size();i<sz; i++ ){
		if((result[i].distance / (1.0*samples)) <= threshold) {
			currentE[result[i].personId] = 1;
			p_++;
		}
	}

	// STEP 5
	do{
		// p will hold the value of the previous set
		p=p_;
		// STEP 7-8
		Rand(persons, graph, k, N_PERSONS, samples,PersonCentralityEstimations,
				visited, Q, &minMax, samples, samples+q, result);
		std::stable_sort(result.begin(), result.end(), EstimationNodeInc);
		if( samples + q > N_PERSONS )
			// TODO - exit here
			samples = N_PERSONS;
		else
			samples += q;

		// STEP 9
		D = std::min(D, 2 * minMax);

		// STEP 11-12 - calculate p_
		threshold = (result.at(k).distance / (1.0*samples)) + 2*f_l*D;
		p_ = 0;
		memset(currentE, 0, N_PERSONS);
		/*
		for (long i = 0; i < N_PERSONS; i++) {
			if ( PersonCentralityEstimations[i] > 0 && (PersonCentralityEstimations[i] / 1.0*samples) <= threshold) {
				currentE[i] = 1;
				p_++;
			}
		}
		*/

		for( long i=0,sz=result.size();i<sz; i++ ){
			if((result[i].distance / (1.0*samples)) <= threshold) {
				currentE[result[i].personId] = 1;
				p_++;
			}
		}


		//temp = previousE;
		//previousE = currentE;
		//currentE = temp;
	}while( p-p_ > q );

	// STEP 14
	/*
	for( long i=0,sz=result.size(); i<sz; i++ ){
		long real_gd = calculateGeodesicDistance(graph, result[i].personId, visited, Q, N_PERSONS);
		result[i].distance = real_gd;
	}
	*/

	vector<EstimationNode> finalRes;
	for( long i=0; i<N_PERSONS; i++ ){
		if( currentE[i] == 1 ){
			long real_gd = calculateGeodesicDistance(graph, i,visited, Q, N_PERSONS);
			finalRes.push_back(EstimationNode(i, real_gd));
		}
	}

	free(PersonCentralityEstimations);
	free(Q);
	free(visited);

	std::stable_sort(finalRes.begin(), finalRes.end(), EstimationNodeInc);
	finalRes.resize( finalRes.size()<(unsigned int)k?finalRes.size():(unsigned int)k );
	vector<std::pair<long,long> > final;
	for( long i=0; i<k; i++ )
		final.push_back(std::pair<long,long>(finalRes[i].personId, finalRes[i].distance));
	return final;
}


















