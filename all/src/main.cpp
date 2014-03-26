/*
 * main.cpp
 *
 *  Created on: Mar 14, 2014
 *      Author: lambros
 */

#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <climits>
#include <sys/time.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>

#include <string>
#include <list>
#include <vector>
#include <queue>
//#include <map>
#include <algorithm>
#include <iterator>
#include <sstream>

#include <tr1/unordered_map>

#include "lplibs/LPBitset.h"
#include "lplibs/LPThreadpool.h"
#include "lplibs/LPSparseBitset.h"
#include "lplibs/LPSparseArrayGeneric.h"
#include "lplibs/atomic_ops_if.h"

using namespace std;
using std::tr1::unordered_map;
using std::tr1::hash;

//#define DEBUGGING 1
#define FILE_VBUF_SIZE 1<<20
#define FILE_BUFFER_SIZE 1<<15

#define CACHE_LINE_SIZE 64

#define VALID_PLACE_CHARS 256
#define LONGEST_LINE_READING 2048

#define NUM_CORES 4
#define WORKER_THREADS NUM_CORES
#define NUM_THREADS WORKER_THREADS+1

#define BATCH_Q1 200
#define BATCH_Q2 20
#define BATCH_Q3 10
#define BATCH_Q4 1

///////////////////////////////////////////////////////////////////////////////
// structs
///////////////////////////////////////////////////////////////////////////////

//typedef map<int, int> MAP_INT_INT;
typedef std::tr1::unordered_map<int, int, hash<int> > MAP_INT_INT;
typedef std::tr1::unordered_map<long, long, hash<long> > MAP_LONG_LONG;
typedef std::tr1::unordered_map<int, vector<long>, hash<int> > MAP_INT_VecL;
typedef std::tr1::unordered_map<long, char*, hash<long> > MAP_LONG_STRING;

struct PersonStruct {
	PersonStruct() {
		subgraphNumber = -1;
		adjacents = 0;
		adjacentPersonsIds = NULL;
		adjacentPersonWeightsSorted = NULL;
	}
	long *adjacentPersonsIds;
	long adjacents;

	long *adjacentPersonWeightsSorted;

	int subgraphNumber;
}__attribute__((aligned(CACHE_LINE_SIZE)));
// Aligned for cache lines;

struct PersonCommentsStruct {
	//MAP_LONG_LONG commentsToPerson;
	//MAP_LONG_LONG adjacentPersonWeights;
	LPSparseArrayGeneric<long> commentsToPerson;
	LPSparseArrayGeneric<long> adjacentPersonWeights;
}__attribute__((aligned(CACHE_LINE_SIZE)));;

struct TrieNode {
	char valid;
	long realId;
	long vIndex;
	TrieNode* children[VALID_PLACE_CHARS];
}__attribute__((aligned(CACHE_LINE_SIZE)));
// Aligned for cache lines;

struct PlaceNodeStruct {
	long id;
	long index;
	vector<long> personsThis;
	vector<long> placesPartOfIndex;
}__attribute__((aligned(CACHE_LINE_SIZE)));
// Aligned for cache lines;

struct PersonTags {
	vector<long> tags;
}__attribute__((aligned(CACHE_LINE_SIZE)));
// Aligned for cache lines;

struct TagNode {
	long id;
	TrieNode *tagNode;
	vector<long> forums;
}__attribute__((aligned(CACHE_LINE_SIZE)));
// Aligned for cache lines;

// final sorted lists
struct Q2ListNode {
	long personId;
	unsigned int birth;
	unsigned int component_size; // maximum cluster from this date - NOT USED YET
	//unsigned int maxBirth; // [0] of the sorted list
	//unsigned int peopleBelow;
};

// intermediate tree map
struct TagSubStruct {
	long tagId;
	long subId;
	vector<Q2ListNode*> people;
}__attribute__((aligned(CACHE_LINE_SIZE)));;

/////////////////////////////
// QUERY SPECIFIC
/////////////////////////////
struct QueryBFS {
	QueryBFS(long id, long d) {
		person = id;
		depth = d;
	}
	long person;
	int depth;
};
// Aligned for cache lines;

struct Query3PQ {
	Query3PQ(long a, long b, int ct) {
		idA = a;
		idB = b;
		commonTags = ct;
	}
	long idA;
	long idB;
	int commonTags;
};
// Aligned for cache lines;
class Query3PQ_Comparator {
public:
	bool operator()(const Query3PQ &left, const Query3PQ &right) {
		if (left.commonTags > right.commonTags)
			return false;
		if (left.commonTags < right.commonTags)
			return true;
		if (left.idA < right.idA)
			return false;
		if (left.idA > right.idA)
			return true;
		if (left.idB <= right.idB)
			return false;
		return true;
	}
};

struct Query4PersonStruct {
	Query4PersonStruct(long id, int sp, int rp, double central) {
		person = id;
		s_p = sp;
		r_p = rp;
		centrality = central;
	}
	long person;
	int s_p;
	int r_p;
	double centrality;
};

bool Query4PersonStructPredicate(const Query4PersonStruct& d1,
		const Query4PersonStruct& d2) {
	if (d1.centrality == d2.centrality)
		return d1.person <= d2.person;
	return d1.centrality > d2.centrality;
}

bool Query4PersonStructPredicateId(const Query4PersonStruct& d1,
		const Query4PersonStruct& d2) {
	// sort in descending order
	return d1.person >= d2.person;
}

bool DescendingIntPredicate(int a, int b) {
	return a >= b;
}

struct Q2ResultNode{
	Q2ResultNode(long tag, long person){
		tagId = tag;
		people = person;
	}
	long tagId;
	long people;
};

bool Q2ListNodePredicate(Q2ListNode* a, Q2ListNode* b) {
	return a->birth >= b->birth;
}

bool Q2ListPredicate(TagSubStruct* a, TagSubStruct* b) {
	return a->people.size() >= b->people.size();
}



///////////////////////////////////////////////////////////////////////////////
// FUNCTION PROTOTYPES
///////////////////////////////////////////////////////////////////////////////

TrieNode* TrieNode_Constructor();
void TrieNode_Destructor(TrieNode* node);
TrieNode* TrieInsert(TrieNode* node, const char* name, char name_sz, long id,
		long index);
TrieNode* TrieFind(TrieNode* root, const char* name, char name_sz);

///////////////////////////////////////////////////////////////////////////////
// GLOBAL STRUCTURES
///////////////////////////////////////////////////////////////////////////////
char *inputDir = "all/input/outputDir-1k";
char *queryFile = "all/queries/1k-queries_utf8.txt";

// required for query 1
char *CSV_PERSON = "/person.csv";
char *CSV_PERSON_KNOWS_PERSON = "/person_knows_person.csv";
char *CSV_COMMENT_HAS_CREATOR = "/comment_hasCreator_person.csv";
char *CSV_COMMENT_REPLY_OF_COMMENT = "/comment_replyOf_comment.csv";

// required for query 3
char *CSV_PLACE = "/place.csv";
char *CSV_PLACE_PART_OF_PLACE = "/place_isPartOf_place.csv";
char *CSV_PERSON_LOCATED_AT_PLACE = "/person_isLocatedIn_place.csv";
char *CSV_ORGANIZATION_LOCATED_AT_PLACE = "/organisation_isLocatedIn_place.csv";

char *CSV_PERSON_STUDYAT_ORG = "/person_studyAt_organisation.csv";
char *CSV_WORKAT_ORG = "/person_workAt_organisation.csv";

char *CSV_TAG = "/tag.csv";
char *CSV_PERSON_HASINTEREST_TAG = "/person_hasInterest_tag.csv";

// Q4
char *CSV_FORUM_HAS_TAG = "/forum_hasTag_tag.csv";
char *CSV_FORUM_HAS_MEMBER = "/forum_hasMember_person.csv";
//char *CSV_FORUM_HAS_MEMBER = "/forum_hasMember_person_mod.csv";

long N_PERSONS = 0;
long N_TAGS = 0;
long N_SUBGRAPHS = 0;
long N_QUERIES = 0;

lp_threadpool *threadpool;
lp_threadpool *threadpool_q1;
lp_threadpool *threadpool_q23;
lp_threadpool *threadpool_q4;

PersonStruct *Persons;
TrieNode *PlacesToId;
vector<PlaceNodeStruct*> Places;
PersonTags *PersonToTags;

vector<TagNode*> Tags;
TrieNode *TagToIndex; // required by Q4

MAP_INT_VecL Forums;
//vector<ForumNodeStruct*> Forums;

vector<string> Answers;

// the structures below are only used as intermediate steps while
// reading the comments files. DO NOT USE THEM ANYWHERE
PersonCommentsStruct *PersonsComments;
MAP_INT_INT *CommentToPerson;

MAP_INT_INT *PlaceIdToIndex;
MAP_INT_INT *OrgToPlace;

MAP_INT_INT *TagIdToIndex;
MAP_LONG_STRING TagIdToName;

// TODO
int *PersonBirthdays;
typedef std::tr1::unordered_map<unsigned long, TagSubStruct*, hash<unsigned long> > MAP_LONG_TSPTR;
//typedef std::map<unsigned long, TagSubStruct*> MAP_LONG_TSPTR;
MAP_LONG_TSPTR *TagSubBirthdays;
vector<TagSubStruct*> TagSubFinals;

///////////////////////////////////////////////////////////////////////////////

//////////////////////////////////////////////////////////////////////////////
// UTILS
/////////////////////////////////////////////////////////////////////////////

void printOut(char* msg) {
	fprintf(stdout, "out:: %s\n", msg);
}

void printErr(char* msg) {
	fprintf(stderr, "err:: %s", msg);
	exit(1);
}

// returns microseconds
long long getTime() {
	struct timeval tim;
	gettimeofday(&tim, NULL);
	long t1 = (tim.tv_sec * 1000000LL) + tim.tv_usec;
	return t1;
}

void mergeCommentsWeights(long *weights, long a[], long low, long mid,
		long high, long *b, long *bWeights) {
	long i = low, j = mid + 1, k = low;

	while (i <= mid && j <= high) {
		//if (weights[i] <= weights[j]){
		if (weights[i] >= weights[j]) {
			b[k] = a[i];
			bWeights[k] = weights[i];
			k++;
			i++;
		} else {
			b[k] = a[j];
			bWeights[k] = weights[j];
			k++;
			j++;
		}
	}
	while (i <= mid) {
		b[k] = a[i];
		bWeights[k] = weights[i];
		k++;
		i++;
	}

	while (j <= high) {
		b[k] = a[j];
		bWeights[k] = weights[j];
		k++;
		j++;
	}

	k--;
	while (k >= low) {
		a[k] = b[k];
		weights[k] = bWeights[k];
		k--;
	}
}

void mergesortComments(long* weights, long a[], long low, long high, long *b,
		long *bWeights) {
	if (low < high) {
		long m = ((high - low) >> 1) + low;
		mergesortComments(weights, a, low, m, b, bWeights);
		mergesortComments(weights, a, m + 1, high, b, bWeights);
		mergeCommentsWeights(weights, a, low, m, high, b, bWeights);
	}
}

long countFileLines(FILE *file) {
	long lines = 0;
	while (EOF != (fscanf(file, "%*[^\n]"), fscanf(file, "%*c")))
		++lines;
	return lines;
}

long countFileLines(char *file) {
	//static const auto BUFFER_SIZE = 16 * 1024;
	int fd = open(file, O_RDONLY);
	if (fd == -1)
		printErr("Error while opening file for line counting");

	/* Advise the kernel of our access pattern.  */
	posix_fadvise(fd, 0, 0, 1);  // FDADVICE_SEQUENTIAL

	char buf[(FILE_BUFFER_SIZE) + 1];
	long lines = 0;
	long bytes_read;

	while ((bytes_read = read(fd, buf, FILE_BUFFER_SIZE)) > 0) {
		if (bytes_read == -1)
			printErr("countFileLines()::Could not read from file!");
		if (!bytes_read)
			break;
		for (char *p = buf;
				(p = (char*) memchr(p, '\n', (buf + bytes_read) - p)); ++p)
			++lines;
	}

	return lines;
}

long getFileSize(FILE *file) {
	fseek(file, 0, SEEK_END);
	long lSize = ftell(file);
	rewind(file);
	return lSize;
}

char* getFileBytes(FILE *file, long *lSize) {
	setvbuf(file, NULL, _IOFBF, FILE_VBUF_SIZE);
	// obtain file size:
	*lSize = getFileSize(file);
	// allocate memory to contain the whole file:
	char* buffer = (char*) malloc(sizeof(char) * *lSize);
	if (buffer == NULL) {
		printErr("getFileBytes:: No memory while reading file!!!");
	}
	// copy the file into the buffer:
	size_t result = fread(buffer, 1, *lSize, file);
	if (result != *lSize) {
		printErr("getFileBytes:: Could not read the whole file in memory!!!");
	}
	return buffer;
}

// converts the date into an integer representing that date
// e.g 1990-07-31 = 19900731
static inline int getDateAsInt(char *date, int date_sz) {
	int dateNum = 0;
	for (int i = 0; i < date_sz; i++) {
		if (date[i] != '-') {
			// dateNum = dateNum*8 + dateNum*2 = dateNum * 10
			dateNum = (dateNum << 3) + (dateNum << 1);
			dateNum += date[i] - '0';
		}
	}
	return dateNum;
}

unsigned long long getDateAsULL(const char *numStr, int num_sz) {
	unsigned long long num = 0;
	for (int i = 0; i < num_sz; i++) {
		// dateNum = dateNum*8 + dateNum*2 = dateNum * 10
		num = (num << 3) + (num << 1);
		num += numStr[i] - '0';
	}
	return num;
}

unsigned long CantorPairingFunction(long k1, long k2) {
	return (((k1 + k2) * (k1 + k2 + 14)) >> 1) + k2;
}

//////////////////////////////////////////////////////////////////////////////

void readPersons(char* inputDir) {
	char path[1024];
	path[0] = '\0';
	strcat(path, inputDir);
	strcat(path, CSV_PERSON);
	FILE *input;

	input = fopen(path, "r");
	if (input == NULL) {
		printErr("could not open person.csv!");
	}
	setvbuf(input, NULL, _IOFBF, FILE_VBUF_SIZE);
	long lines = countFileLines(input);
	fclose(input);
	//long lines = countFileLines(path);
	N_PERSONS = lines - 1;

#ifdef DEBUGGING
	char msg[100];
	sprintf(msg, "Total persons: %d", N_PERSONS);
	printOut(msg);
#endif

	// TODO - DO IT IN ONE FILE OPEN
	// TODO - read birthdays
	PersonBirthdays = (int*) malloc(sizeof(int) * N_PERSONS);
	input = fopen(path, "r");
	if (input == NULL) {
		printErr("could not open person.csv!");
	}
	long lSize;
	char *buffer = getFileBytes(input, &lSize);

	// process the whole file in memory
	// skip the first line
	char *startLine = ((char*) memchr(buffer, '\n', LONGEST_LINE_READING)) + 1;
	char *EndOfFile = buffer + lSize;
	char *lineEnd;
	char *dateStartDivisor;
	while (startLine < EndOfFile) {
		//lineEnd = (char*) memchr(startLine, '\n', LONGEST_LINE_READING);
		//for( dateStartDivisor=startLine; *dateStartDivisor != '|'; dateStartDivisor++);
		dateStartDivisor = (char*) memchr(startLine, '|', LONGEST_LINE_READING);
		*dateStartDivisor = '\0';
		long idPerson = atol(startLine);
		dateStartDivisor = (char*) memchr(dateStartDivisor + 1, '|', LONGEST_LINE_READING);
		dateStartDivisor = (char*) memchr(dateStartDivisor + 1, '|', LONGEST_LINE_READING);
		dateStartDivisor = (char*) memchr(dateStartDivisor + 1, '|', LONGEST_LINE_READING);
		/*
		int divisorsCount = 0;
		for( dateStartDivisor=dateStartDivisor+1; divisorsCount<3; dateStartDivisor++ ){
			if( *dateStartDivisor == '|' )
				divisorsCount++;
		}
		*/

		int dateInt = getDateAsInt(dateStartDivisor + 1, 10);
		//printf("%d\n", dateInt);
		PersonBirthdays[idPerson] = dateInt;

		//for( lineEnd=dateStartDivisor+10; *lineEnd != '\n'; lineEnd++);
		lineEnd = (char*) memchr(dateStartDivisor+10, '\n', LONGEST_LINE_READING);
		startLine = lineEnd + 1;
	}
	// close the comment_hasCreator_Person
	fclose(input);
	free(buffer);

	// initialize persons
	//Persons = malloc(sizeof(PersonStruct)*N_PERSONS);
	Persons = new PersonStruct[N_PERSONS];
	PersonsComments = new PersonCommentsStruct[N_PERSONS];
	PersonToTags = new PersonTags[N_PERSONS];
}

long calculateAndAssignSubgraphs() {
	char *visited = (char*) malloc(N_PERSONS);
	memset(visited, 0, N_PERSONS);
	long currentSubgraph = 0;
	for (long cPerson = 0, sz = N_PERSONS; cPerson < sz; cPerson++) {
		if (visited[cPerson] != 0)
			continue;
		// start BFS from the current person
		vector<long> Q;
		Q.push_back(cPerson);
		long qIndex = 0;
		long qSize = 1;
		while (qIndex < qSize) {
			long cP = Q[qIndex];
			qIndex++;
			// set as visited
			visited[cP] = 2;
			// this person belongs to the current subgraph being traversed
			Persons[cP].subgraphNumber = currentSubgraph;
			long *adjacents = Persons[cP].adjacentPersonsIds;
			for (long cAdjacent = 0, szz = Persons[cP].adjacents;
					cAdjacent < szz; cAdjacent++) {
				long neighbor = adjacents[cAdjacent];
				// check if not visited nor added
				if (visited[neighbor] == 0) {
					// set as added
					visited[neighbor] = 1;
					Q.push_back(neighbor);
					qSize++;
				}
			}
		}
		// increase the subgraphs
		currentSubgraph++;
	}
	free(visited);
#ifdef DEBUGGING
	char msg[100];
	sprintf(msg, "Total subgraphs: %ld", currentSubgraph);
	printOut(msg);
#endif
	return currentSubgraph;
}

void readPersonKnowsPerson(char *inputDir) {
	char path[1024];
	path[0] = '\0';
	strcat(path, inputDir);
	strcat(path, CSV_PERSON_KNOWS_PERSON);
	FILE *input = fopen(path, "r");
	if (input == NULL) {
		printErr("could not open person_knows_person!");
	}
	setvbuf(input, NULL, _IOFBF, FILE_VBUF_SIZE);

	// obtain file size:
	long lSize = getFileSize(input);

	// allocate memory to contain the whole file:
	char* buffer = (char*) malloc(sizeof(char) * lSize);
	if (buffer == NULL) {
		printErr(
				"readPersonKnowsPerson:: No memory while reading Person_Knows_Person!!!");
	}

	// copy the file into the buffer:
	size_t result = fread(buffer, 1, lSize, input);
	if (result != lSize) {
		printErr(
				"readPersonKnowsPerson:: Could not read the whole file in memory!!!");
	}

	long edges = 0;

	// the whole file is now loaded in the memory buffer.
	vector<long> ids;
	ids.reserve(128);
	long prevId = -1;
	// skip the first line
	char *startLine = ((char*) memchr(buffer, '\n', LONGEST_LINE_READING)) + 1;
	char *EndOfFile = buffer + lSize;
	while (startLine < EndOfFile) {
		char *idDivisor = (char*) memchr(startLine, '|', LONGEST_LINE_READING);
		char *lineEnd = (char*) memchr(idDivisor, '\n', LONGEST_LINE_READING);
		*idDivisor = '\0';
		*lineEnd = '\0';
		long idA = atol(startLine);
		long idB = atol(idDivisor + 1);
		//printf("%d %d\n", idA, idB);

		if (idA != prevId) {
			if (ids.size() > 0) {
				// store the neighbors
				//Persons[idA].adjacentPersons = ids;
				PersonStruct *person = &Persons[prevId];
				person->adjacentPersonsIds = (long*) malloc(
						sizeof(long) * ids.size());
				for (long i = 0, sz = ids.size(); i < sz; i++) {
					person->adjacentPersonsIds[i] = ids[i];
				}
				person->adjacents = ids.size();
				ids.clear();
			}
		}
		prevId = idA;
		ids.push_back(idB);
		startLine = lineEnd + 1;
#ifdef DEBUGGING
		edges++;
#endif
	}

	// check if there are edges to be added for the last person
	if (!ids.empty()) {
		PersonStruct *person = &Persons[prevId];
		person->adjacentPersonsIds = (long*) malloc(sizeof(long) * ids.size());
		for (int i = 0, sz = ids.size(); i < sz; i++) {
			person->adjacentPersonsIds[i] = ids[i];
		}
		person->adjacents = ids.size();
	}

	free(buffer);

#ifdef DEBUGGING
	char msg[100];
	sprintf(msg, "Total edges: %d", edges);
	printOut(msg);
#endif

	// now we want to find all the subgraphs into the graph and assign each person
	// into one of them since we will use this info into the Query 4
	N_SUBGRAPHS = calculateAndAssignSubgraphs();
}

void postProcessComments() {
	// for each person we will get each neighbor and put our edge weight in an array
	// to speed up look up time and then sort them
	for (long i = 0, sz = N_PERSONS; i < sz; i++) {
		if (Persons[i].adjacents > 0) {
			long adjacents = Persons[i].adjacents;
			long *adjacentIds = Persons[i].adjacentPersonsIds;
			//MAP_LONG_LONG *weightsMap = &(PersonsComments[i].adjacentPersonWeights);
			LPSparseArrayGeneric<long> *weightsMap = &(PersonsComments[i].adjacentPersonWeights);
			Persons[i].adjacentPersonWeightsSorted = (long*) malloc(sizeof(long) * adjacents);
			long *weights = Persons[i].adjacentPersonWeightsSorted;
			for (long cAdjacent = 0, szz = adjacents; cAdjacent < szz; cAdjacent++) {
				weights[cAdjacent] = (*(weightsMap)).get(adjacentIds[cAdjacent]);
				//weights[cAdjacent] = (*(weightsMap))[adjacentIds[cAdjacent]];
			}
			// now we need to sort them
			/*
			 printf("\n\nUnsorted: \n");
			 for( long cAdjacent=0,szz=adjacents; cAdjacent<szz; cAdjacent++){
			 printf("[%ld,%ld] ",Persons[i].adjacentPersonsIds[cAdjacent], weights[cAdjacent]);
			 }
			 */
			long *temp = (long*) malloc(sizeof(long) * (adjacents));
			long *tempWeights = (long*) malloc(sizeof(long) * (adjacents));
			mergesortComments(weights, adjacentIds, 0, adjacents - 1, temp,	tempWeights);
			free(temp);
			free(tempWeights);
			/*
			 printf("\nSorted: \n");
			 for( long cAdjacent=0,szz=adjacents; cAdjacent<szz; cAdjacent++){
			 printf("[%ld,%ld] ",Persons[i].adjacentPersonsIds[cAdjacent], Persons[i].adjacentPersonWeightsSorted[cAdjacent]);
			 }
			 */
		}
	}
	// since we have all the data needed in arrays we can delete the hash maps
	CommentToPerson->clear();
	delete CommentToPerson;
	CommentToPerson = NULL;
	delete[] PersonsComments;
	PersonsComments = NULL;
}

void readComments(char* inputDir) {
	char path[1024];

	///////////////////////////////////////////////////////////////////
	// READ THE COMMENTS AGAINST EACH PERSON
	///////////////////////////////////////////////////////////////////
	path[0] = '\0';
	strcat(path, inputDir);
	strcat(path, CSV_COMMENT_HAS_CREATOR);
	FILE *input = fopen(path, "r");
	if (input == NULL) {
		printErr("could not open comment_hasCreator_person.csv!");
	}
	long lSize;
	char *buffer = getFileBytes(input, &lSize);

#ifdef DEBUGGING
	char msg[100];
	long comments=0;
#endif

	// process the whole file in memory
	// skip the first line
	char *startLine = ((char*) memchr(buffer, '\n', LONGEST_LINE_READING)) + 1;
	char *EndOfFile = buffer + lSize;
	char *lineEnd;
	char *idDivisor;
	while (startLine < EndOfFile) {
		idDivisor = (char*) memchr(startLine, '|', LONGEST_LINE_READING);
		lineEnd = (char*) memchr(idDivisor, '\n', LONGEST_LINE_READING);
		*idDivisor = '\0';
		*lineEnd = '\0';
		long idA = atol(startLine);
		long idB = atol(idDivisor + 1);

		// set the person to each comment
		(*CommentToPerson)[idA] = idB;

		//printf("%d %d\n", idA, idB);

		startLine = lineEnd + 1;
#ifdef DEBUGGING
		comments++;
#endif
	}
	// close the comment_hasCreator_Person
	fclose(input);
	free(buffer);

#ifdef DEBUGGING
	sprintf(msg, "Total comments: %ld", comments);
	printOut(msg);
#endif

	///////////////////////////////////////////////////////////////////
	// READ THE COMMENT REPLIES TO HAVE THE COMMENTS FOR EACH PERSON
	///////////////////////////////////////////////////////////////////
	path[0] = '\0';
	strcat(path, inputDir);
	strcat(path, CSV_COMMENT_REPLY_OF_COMMENT);
	input = fopen(path, "r");
	if (input == NULL) {
		printErr("could not open comment_replyOf_Comment.csv!");
	}
	buffer = getFileBytes(input, &lSize);

#ifdef DEBUGGING
	comments=0;
#endif

	// process the whole file in memory
	// skip the first line
	startLine = ((char*) memchr(buffer, '\n', 100)) + 1;
	EndOfFile = buffer + lSize;
	while (startLine < EndOfFile) {
		idDivisor = (char*) memchr(startLine, '|', LONGEST_LINE_READING);
		lineEnd = (char*) memchr(idDivisor, '\n', LONGEST_LINE_READING);
		*idDivisor = '\0';
		*lineEnd = '\0';
		long idA = atol(startLine);
		long idB = atol(idDivisor + 1);

		// we have to hold the number of comments between each person
		long personA = (*CommentToPerson)[idA];
		long personB = (*CommentToPerson)[idB];

		if (personA != personB) {
			// increase the counter for the comments from A to B
			int a_b = PersonsComments[personA].commentsToPerson.get(personB) + 1;
			PersonsComments[personA].commentsToPerson.set(personB, a_b);
			//int a_b = PersonsComments[personA].commentsToPerson[personB] + 1;
			//PersonsComments[personA].commentsToPerson[personB] = a_b;

			///////////////////////////////////////////////////////////////////
			// - Leave only the min(comments A-to-B, comments B-to-A) at each edge
			///////////////////////////////////////////////////////////////////
			int b_a = PersonsComments[personB].commentsToPerson.get(personA);
			//int b_a = PersonsComments[personB].commentsToPerson[personA];
			if (a_b <= b_a) {
				PersonsComments[personA].adjacentPersonWeights.set(personB, a_b);
				PersonsComments[personB].adjacentPersonWeights.set(personA, a_b);
				//PersonsComments[personA].adjacentPersonWeights[personB] = a_b;
				//PersonsComments[personB].adjacentPersonWeights[personA] = a_b;
			}
		}
		//printf("%ld %ld %ld\n", idA, idB, Persons[personA].commentsToPerson[personB] );

		startLine = lineEnd + 1;
#ifdef DEBUGGING
		comments++;
#endif
	}
	free(buffer);

#ifdef DEBUGGING
	sprintf(msg, "Total replies: %ld", comments);
	printOut(msg);
#endif

}

void readPlaces(char *inputDir) {
	char path[1024];
	path[0] = '\0';
	strcat(path, inputDir);
	strcat(path, CSV_PLACE);
	FILE *input = fopen(path, "r");
	if (input == NULL) {
		printErr("could not open place.csv!");
	}
	long lSize;
	char *buffer = getFileBytes(input, &lSize);

#ifdef DEBUGGING
	char msg[100];
#endif

	long places = 0;
	// process the whole file in memory
	// skip the first line
	char *startLine = ((char*) memchr(buffer, '\n', LONGEST_LINE_READING)) + 1;
	char *EndOfFile = buffer + lSize;
	char *lineEnd;
	char *idDivisor;
	char *nameDivisor;
	while (startLine < EndOfFile) {
		idDivisor = (char*) memchr(startLine, '|', LONGEST_LINE_READING);
		nameDivisor = (char*) memchr(idDivisor + 1, '|', LONGEST_LINE_READING);
		*idDivisor = '\0';
		*nameDivisor = '\0';
		long id = atol(startLine);
		char *name = idDivisor + 1;

		// insert the place into the Trie for PlacesToId
		// we first insert into the trie in order to get the Place node that already exists if any
		// for this place, or the new one that was created with this insertion.
		// this way we will always get the same index for the same place name regardless of id
		TrieNode *insertedPlace = TrieInsert(PlacesToId, name,
				nameDivisor - name, id, places);
		// create a new Place structure only if this was a new Place and not an existing place with
		// a different id, like Asia or Brasil
		if (insertedPlace->realId == id) {
			PlaceNodeStruct *node = new PlaceNodeStruct();
			node->id = id;
			node->index = insertedPlace->vIndex;
			Places.push_back(node);
			places++;
		}
		// map the place id to the place index
		(*PlaceIdToIndex)[id] = insertedPlace->vIndex;

		//printf("place[%ld] name[%*s] index[%ld] idToIndex[%ld]\n", id, nameDivisor-name, name,  insertedPlace->placeIndex, (*PlaceIdToIndex)[id]);

		lineEnd = (char*) memchr(nameDivisor, '\n', LONGEST_LINE_READING);
		startLine = lineEnd + 1;
	}
	// close the comment_hasCreator_Person
	fclose(input);
	free(buffer);

#ifdef DEBUGGING
	sprintf(msg, "Total places: %ld", places);
	printOut(msg);
#endif
}

void readPlacePartOfPlace(char *inputDir) {
	char path[1024];
	path[0] = '\0';
	strcat(path, inputDir);
	strcat(path, CSV_PLACE_PART_OF_PLACE);
	FILE *input = fopen(path, "r");
	if (input == NULL) {
		printErr("could not open place_isPartOf_place.csv!");
	}
	long lSize;
	char *buffer = getFileBytes(input, &lSize);

#ifdef DEBUGGING
	long places=0;
	char msg[100];
#endif

	// process the whole file in memory
	// skip the first line
	char *startLine = ((char*) memchr(buffer, '\n', LONGEST_LINE_READING)) + 1;
	char *EndOfFile = buffer + lSize;
	char *lineEnd;
	char *idDivisor;
	while (startLine < EndOfFile) {
		idDivisor = (char*) memchr(startLine, '|', LONGEST_LINE_READING);
		lineEnd = (char*) memchr(idDivisor, '\n', LONGEST_LINE_READING);
		*idDivisor = '\0';
		*lineEnd = '\0';
		long idA = atol(startLine);
		long idB = atol(idDivisor + 1);

		if (idA != idB) {
			// insert the place idA into the part of place idB
			long indexA = (*PlaceIdToIndex)[idA];
			long indexB = (*PlaceIdToIndex)[idB];
			if (indexA != indexB) {
				Places[indexB]->placesPartOfIndex.push_back(indexA);
			}
		}
		//printf("%ld %ld\n", idA, idB);

		startLine = lineEnd + 1;
#ifdef DEBUGGING
		places++;
#endif
	}
	// close the comment_hasCreator_Person
	fclose(input);
	free(buffer);

#ifdef DEBUGGING
	sprintf(msg, "Total places being part of another place: %ld", places);
	printOut(msg);
#endif

}

void readPersonLocatedAtPlace(char *inputDir) {
	char path[1024];
	path[0] = '\0';
	strcat(path, inputDir);
	strcat(path, CSV_PERSON_LOCATED_AT_PLACE);
	FILE *input = fopen(path, "r");
	if (input == NULL) {
		printErr("could not open person_isLocatedIN_place.csv!");
	}
	long lSize;
	char *buffer = getFileBytes(input, &lSize);

#ifdef DEBUGGING
	long persons=0;
	char msg[100];
#endif

	// process the whole file in memory
	// skip the first line
	char *startLine = ((char*) memchr(buffer, '\n', LONGEST_LINE_READING)) + 1;
	char *EndOfFile = buffer + lSize;
	char *lineEnd;
	char *idDivisor;
	while (startLine < EndOfFile) {
		idDivisor = (char*) memchr(startLine, '|', LONGEST_LINE_READING);
		lineEnd = (char*) memchr(idDivisor, '\n', LONGEST_LINE_READING);
		*idDivisor = '\0';
		*lineEnd = '\0';
		long idPerson = atol(startLine);
		long idPlace = atol(idDivisor + 1);

		// insert the place idA into the part of place idB
		long indexPlace = (*PlaceIdToIndex)[idPlace];
		Places[indexPlace]->personsThis.push_back(idPerson);
		//printf("person[%ld] placeId[%ld] placeIndex[%ld]\n", idPerson, idPlace, indexPlace);

		startLine = lineEnd + 1;
#ifdef DEBUGGING
		persons++;
#endif
	}
	// close the comment_hasCreator_Person
	fclose(input);
	free(buffer);

#ifdef DEBUGGING

	long c=0;
	for(unsigned long i=0; i<Places.size(); i++) {
		c += Places[i]->personsThis.size();
		//printf("%ld - %ld\n", i, Places[i]->personsThis.size());
	}

	sprintf(msg, "Total persons located at place: %ld found inserted[%ld]", persons, c);
	printOut(msg);
#endif
}

void readOrgsLocatedAtPlace(char *inputDir) {
	char path[1024];
	path[0] = '\0';
	strcat(path, inputDir);
	strcat(path, CSV_ORGANIZATION_LOCATED_AT_PLACE);
	FILE *input = fopen(path, "r");
	if (input == NULL) {
		printErr("could not open organization_isLocatedIn_place.csv!");
	}
	long lSize;
	char *buffer = getFileBytes(input, &lSize);

#ifdef DEBUGGING
	long orgs=0;
	char msg[100];
#endif

	// process the whole file in memory
	// skip the first line
	char *startLine = ((char*) memchr(buffer, '\n', LONGEST_LINE_READING)) + 1;
	char *EndOfFile = buffer + lSize;
	char *lineEnd;
	char *idDivisor;
	while (startLine < EndOfFile) {
		idDivisor = (char*) memchr(startLine, '|', LONGEST_LINE_READING);
		lineEnd = (char*) memchr(idDivisor, '\n', LONGEST_LINE_READING);
		*idDivisor = '\0';
		*lineEnd = '\0';
		long idOrg = atol(startLine);
		long idPlace = atol(idDivisor + 1);

		// insert the place idA into the part of place idB
		long indexPlace = (*PlaceIdToIndex)[idPlace];
		(*OrgToPlace)[idOrg] = indexPlace;
		//printf("orgId[%ld] placeId[%ld] placeIndex[%ld]\n", idOrg, idPlace, indexPlace);

		startLine = lineEnd + 1;
#ifdef DEBUGGING
		orgs++;
#endif
	}
	// close the comment_hasCreator_Person
	fclose(input);
	free(buffer);

#ifdef DEBUGGING
	sprintf(msg, "Total organizations located at place: %ld", orgs);
	printOut(msg);
#endif


}

void readPersonWorksStudyAtOrg(char *inputDir) {
	char path[1024];
	char *paths[2] = { CSV_PERSON_STUDYAT_ORG, CSV_WORKAT_ORG };

	// we need tod this twice for the files above
	for (int i = 0; i < 2; i++) {
		path[0] = '\0';
		strcat(path, inputDir);
		strcat(path, paths[i]);
		FILE *input = fopen(path, "r");
		if (input == NULL) {
			printErr("could not open personToOrganisation file!");
		}
		long lSize;
		char *buffer = getFileBytes(input, &lSize);

#ifdef DEBUGGING
		long persons = 0;
		char msg[100];
#endif

		// process the whole file in memory
		// skip the first line
		char *startLine = ((char*) memchr(buffer, '\n', LONGEST_LINE_READING)) + 1;
		char *EndOfFile = buffer + lSize;
		char *lineEnd;
		char *idDivisor;
		char *orgDivisor;
		while (startLine < EndOfFile) {
			idDivisor = (char*) memchr(startLine, '|', LONGEST_LINE_READING);
			orgDivisor = (char*) memchr(idDivisor + 1, '|', LONGEST_LINE_READING);
			*idDivisor = '\0';
			*orgDivisor = '\0';
			long idPerson = atol(startLine);
			long idOrg = atol(idDivisor + 1);

			// insert the place idA into the part of place idB
			long indexPlace = (*OrgToPlace)[idOrg];
			Places[indexPlace]->personsThis.push_back(idPerson);
			//printf("person[%ld] org[%ld] place[%ld]\n", idPerson, idOrg, indexPlace);

			lineEnd = (char*) memchr(orgDivisor, '\n', LONGEST_LINE_READING);
			startLine = lineEnd + 1;
#ifdef DEBUGGING
			persons++;
#endif
		}
		// close the comment_hasCreator_Person
		fclose(input);
		free(buffer);

#ifdef DEBUGGING
		sprintf(msg, "Total persons work/study at org: %ld", persons);
		printOut(msg);
#endif

	}		// end of file processing

			// safe to delete this vector since we do not need it anymore
	OrgToPlace->clear();
	delete OrgToPlace;
	OrgToPlace = NULL;
}

void readPersonHasInterestTag(char *inputDir) {
	char path[1024];
	path[0] = '\0';
	strcat(path, inputDir);
	strcat(path, CSV_PERSON_HASINTEREST_TAG);
	FILE *input = fopen(path, "r");
	if (input == NULL) {
		printErr("could not open person_hasInterest_tag.csv!");
	}
	long lSize;
	char *buffer = getFileBytes(input, &lSize);

#ifdef DEBUGGING
	long personHasTag=0;
	char msg[100];
#endif

	TagSubBirthdays = new MAP_LONG_TSPTR();

	// process the whole file in memory
	// skip the first line
	char *startLine = ((char*) memchr(buffer, '\n', LONGEST_LINE_READING)) + 1;
	char *EndOfFile = buffer + lSize;
	char *lineEnd;
	char *idDivisor;

	while (startLine < EndOfFile) {
		idDivisor = (char*) memchr(startLine, '|', LONGEST_LINE_READING);
		lineEnd = (char*) memchr(idDivisor, '\n', LONGEST_LINE_READING);
		*idDivisor = '\0';
		*lineEnd = '\0';
		long personId = atol(startLine);
		long tagId = atol(idDivisor + 1);

		PersonToTags[personId].tags.push_back(tagId);
		//printf("%ld %ld\n", idA, idB);

		int subgraph = Persons[personId].subgraphNumber;
		unsigned long key = CantorPairingFunction(tagId, subgraph);
		if (TagSubBirthdays->find(key) == TagSubBirthdays->end()) {
			TagSubStruct *newTag = new TagSubStruct();
			newTag->subId = subgraph;
			newTag->tagId = tagId;
			(*TagSubBirthdays)[key] = newTag;
		}
		Q2ListNode *newPerson = new Q2ListNode();
		newPerson->birth = PersonBirthdays[personId];
		newPerson->personId = personId;
		newPerson->component_size = 0;
		(*TagSubBirthdays)[key]->people.push_back(newPerson);

		startLine = lineEnd + 1;
#ifdef DEBUGGING
		personHasTag++;
#endif
	}
	// close the comment_hasCreator_Person
	fclose(input);
	free(buffer);

	// delete not required structures anymore - Q2
	free(PersonBirthdays);

	// sort the tags to make easy the comparison
	// TODO - create signatures for the tags instead
	for (long i = 0; i < N_PERSONS; i++) {
		std::stable_sort(PersonToTags[i].tags.begin(),
				PersonToTags[i].tags.end());
	}

#ifdef DEBUGGING
	sprintf(msg, "Total person tags : %ld", personHasTag);
	printOut(msg);
#endif

}

void readTags(char *inputDir) {
	char path[1024];
	path[0] = '\0';
	strcat(path, inputDir);
	strcat(path, CSV_TAG);
	FILE *input = fopen(path, "r");
	if (input == NULL) {
		printErr("could not open tag.csv!");
	}
	long lSize;
	char *buffer = getFileBytes(input, &lSize);

#ifdef DEBUGGING
	char msg[100];
#endif

	long tags = 0;
	// process the whole file in memory
	// skip the first line
	char *startLine = ((char*) memchr(buffer, '\n', LONGEST_LINE_READING)) + 1;
	char *EndOfFile = buffer + lSize;
	char *lineEnd;
	char *idDivisor;
	char *nameDivisor;
	while (startLine < EndOfFile) {
		idDivisor = (char*) memchr(startLine, '|', LONGEST_LINE_READING);
		nameDivisor = (char*) memchr(idDivisor + 1, '|', LONGEST_LINE_READING);
		*idDivisor = '\0';
		*lineEnd = '\0';
		*nameDivisor = '\0';
		long id = atol(startLine);
		char *name = idDivisor + 1;
		int name_sz = nameDivisor - name;

		// insert the tag into the Trie for TagToIndex
		// we first insert into the trie in order to get the Place node that already exists if any
		// for this place, or the new one that was created with this insertion.
		// this way we will always get the same index for the same place name regardless of id
		TrieNode *insertedTag = TrieInsert(TagToIndex, name, name_sz, id, tags);
		// create a new Place structure only if this was a new Place and not an existing place with
		// a different id, like Asia or Brazil
		if (insertedTag->realId == id) {
			TagNode *node = new TagNode();
			node->id = id;
			node->tagNode = insertedTag;
			Tags.push_back(node);
			tags++;
		}
		// map the place id to the place index
		(*TagIdToIndex)[id] = insertedTag->vIndex;
		//printf("tag[%ld] name[%*s] index[%ld]\n", id, nameDivisor-name, name,  insertedTag->vIndex);

		// TODO - SAVE THE TAG NAME
		char *tagName = (char*)malloc(name_sz+1);
		strncpy(tagName, name, name_sz);
		tagName[name_sz] = '\0';
		TagIdToName[id] = tagName;

		lineEnd = (char*) memchr(nameDivisor, '\n', LONGEST_LINE_READING);
		startLine = lineEnd + 1;
	}
	// close the comment_hasCreator_Person
	fclose(input);
	free(buffer);

	N_TAGS = tags;

#ifdef DEBUGGING
	sprintf(msg, "Total tags: %ld", tags);
	printOut(msg);
#endif
}

void readForumHasTag(char *inputDir) {
	char path[1024];
	path[0] = '\0';
	strcat(path, inputDir);
	strcat(path, CSV_FORUM_HAS_TAG);
	FILE *input = fopen(path, "r");
	if (input == NULL) {
		printErr("could not open forum_hasTag_tag.csv!");
	}
	long lSize;
	char *buffer = getFileBytes(input, &lSize);

#ifdef DEBUGGING
	long forumTags=0;
	char msg[100];
#endif

	// process the whole file in memory
	// skip the first line
	char *startLine = ((char*) memchr(buffer, '\n', LONGEST_LINE_READING)) + 1;
	char *EndOfFile = buffer + lSize;
	char *lineEnd;
	char *idDivisor;
	while (startLine < EndOfFile) {
		idDivisor = (char*) memchr(startLine, '|', LONGEST_LINE_READING);
		lineEnd = (char*) memchr(idDivisor, '\n', LONGEST_LINE_READING);
		*idDivisor = '\0';
		*lineEnd = '\0';
		long forumId = atol(startLine);
		long tagId = atol(idDivisor + 1);

		// insert the forum into the tag
		long tagIndex = (*TagIdToIndex)[tagId];
		Tags[tagIndex]->forums.push_back(forumId);
		//printf("%ld %ld\n", idA, idB);

		startLine = lineEnd + 1;
#ifdef DEBUGGING
		forumTags++;
#endif
	}
	// close the comment_hasCreator_Person
	fclose(input);
	free(buffer);

#ifdef DEBUGGING
	sprintf(msg, "Total forums having tags: %ld", forumTags);
	printOut(msg);
#endif

	// now we can delete the TagIds
	delete TagIdToIndex;
}

void readForumHasMember(char *inputDir) {
	char path[1024];
	path[0] = '\0';
	strcat(path, inputDir);
	strcat(path, CSV_FORUM_HAS_MEMBER);
	FILE *input = fopen(path, "r");
	if (input == NULL) {
		printErr("could not open forum_hasMember_person.csv!");
	}
	long lSize;
	char *buffer = getFileBytes(input, &lSize);

#ifdef DEBUGGING
	long forumPersons=0;
	char msg[100];
#endif
	// process the whole file in memory
	// skip the first line
	char *startLine = ((char*) memchr(buffer, '\n', LONGEST_LINE_READING)) + 1;
	char *EndOfFile = buffer + lSize;
	char *lineEnd;
	char *idDivisor;
	char *dateDivisor;
	while (startLine < EndOfFile) {
		idDivisor = (char*) memchr(startLine, '|', LONGEST_LINE_READING);
		dateDivisor = (char*) memchr(idDivisor + 1, '|', LONGEST_LINE_READING);
		*idDivisor = '\0';
		*dateDivisor = '\0';
		long forumId = atol(startLine);
		long personId = atol(idDivisor + 1);

		// insert the person directly into the forum members
		Forums[forumId].push_back(personId);

		lineEnd = (char*) memchr(dateDivisor, '\n', LONGEST_LINE_READING);
		startLine = lineEnd + 1;
#ifdef DEBUGGING
		forumPersons++;
#endif
	}
	// close the comment_hasCreator_Person
	fclose(input);
	free(buffer);

#ifdef DEBUGGING
	sprintf(msg, "Total persons members of forums: %ld", forumPersons);
	printOut(msg);
#endif
}

///////////////////////////////////////////////////////////////////////
// PROCESSING FUNCTIONS
///////////////////////////////////////////////////////////////////////

void postProcessTagBirthdays() {
	for (MAP_LONG_TSPTR::iterator it = TagSubBirthdays->begin(),
			end = TagSubBirthdays->end(); it != end;	it++) {
		TagSubStruct*tagStruct = (*it).second;
		TagSubFinals.push_back(tagStruct);
		// sort the birthdays of each TagSubgraph
		std::stable_sort(tagStruct->people.begin(), tagStruct->people.end(),
				Q2ListNodePredicate);
	}
	// sort the final list of tags descending on the list size
	std::stable_sort(TagSubFinals.begin(), TagSubFinals.end(), Q2ListPredicate);

	// dynamic memory allocation to DELETE
	delete TagSubBirthdays;
	//TagSubBirthdays.clear();
}



///////////////////////////////////////////////////////////////////////
///////////////////// READING FILES - WORKER JOBS /////////////////////
///////////////////////////////////////////////////////////////////////

// TODO - optimization instead of adding all the jobs : KEEP ONE FOR YOURSELF

int phase2_placesFinished = 0;

void* phase3_ReadForumHasTags(int tid, void *args){
	readForumHasTag(inputDir);
	return 0;
}

void* phase3_ReadForumHasMembers(int tid, void *args){
	readForumHasMember(inputDir);
	return 0;
}

void* phase2_ReadPlacePartOfPlace(int tid, void *args){
	readPlacePartOfPlace(inputDir);
	phase2_placesFinished = 1;
	//FAI_U32(&phase2_placesFinished);
	return 0;
}

void* phase2_ReadTagsAndPersonTags(int tid, void* args){
	readPersonHasInterestTag(inputDir);
	// Q2 - create the final arrays
	postProcessTagBirthdays();

	readTags(inputDir);
	// add the rest of tag functions
	lp_threadpool_addjob(threadpool,reinterpret_cast<void* (*)(int,void*)>(phase3_ReadForumHasTags), NULL );
	phase3_ReadForumHasMembers(tid, NULL);
	return 0;
}

void* phase2_ReadComments(int tid, void *args){
	readComments(inputDir);
	postProcessComments();
	return 0;
}

void* phase1_ReadPersons(int tid, void *args){
	readPersons(inputDir);
	readPersonKnowsPerson(inputDir);

	lp_threadpool_addjob(threadpool,reinterpret_cast<void* (*)(int,void*)>(phase2_ReadComments), NULL );
	// process the tags now that we have persons
	phase2_ReadTagsAndPersonTags(tid, NULL);

	return 0;
}

void* phase1_ReadPlacesFiles(int tid, void* args){
	readPlaces(inputDir);
	// add the rest of places jobs
	lp_threadpool_addjob(threadpool,reinterpret_cast<void* (*)(int,void*)>(phase2_ReadPlacePartOfPlace), NULL );

	// execute one job too
	readPersonLocatedAtPlace(inputDir);
	readOrgsLocatedAtPlace(inputDir);
	readPersonWorksStudyAtOrg(inputDir);

	// now we can delete PlaceId to Index hashmap since no further
	// data will come containing the PlaceId
	while( phase2_placesFinished == 0 );

	delete PlaceIdToIndex;
	PlaceIdToIndex = NULL;

	return 0;
}












///////////////////////////////////////////////////////////////////////
// QUERY EXECUTORS
///////////////////////////////////////////////////////////////////////

void query1(int p1, int p2, int x, long qid) {
	//printf("query1: %d %d %d\n", p1, p2, x);

	if(p1 == p2){
		std::stringstream ss;
		ss << 0;
		Answers[qid] = ss.str();
		return;
	}else if( p1 < 0 || p2 < 0 ){
		std::stringstream ss;
		ss << -1;
		Answers[qid] = ss.str();
		return;
	}

	int answer = -1;

	//char *visited = (char*) malloc(N_PERSONS);
	//memset(visited, 0, N_PERSONS);
	LPSparseArrayGeneric<long> visited;
	vector<QueryBFS> Q;

	// insert the source node into the queue
	Q.push_back(QueryBFS(p1, 0));
	unsigned long index = 0;
	unsigned long size = 1;
	while (index < size) {
		QueryBFS current = Q[index];
		index++;

		//printf("current: %ld %d\n", current.person, current.depth);
		// mark node as visited - BLACK
		//visited[current.person] = 2;
		visited.set(current.person,2);

		// we must add the current neighbors into the queue if
		// the comments are valid
		PersonStruct *cPerson = &Persons[current.person];
		long *adjacents = cPerson->adjacentPersonsIds;
		long *weights = cPerson->adjacentPersonWeightsSorted;
		// if there is comments limit
		if (x != -1) {
			for (long i = 0, sz = cPerson->adjacents;
					(i < sz) && (weights[i] > x); i++) {
				long cAdjacent = adjacents[i];
				//if (visited[cAdjacent] == 0) {
				if (visited.get(cAdjacent) == 0) {
					if (cAdjacent == p2) {
						answer = current.depth + 1;
						break;
					}
					visited.set(cAdjacent,1);
					Q.push_back(QueryBFS(cAdjacent, current.depth + 1));
					size++;
				}
			}
		} else {
			// no comments limit
			for (long i = 0, sz = cPerson->adjacents; i < sz; i++) {
				long cAdjacent = adjacents[i];
				// if node not visited and not added
				//if (visited[cAdjacent] == 0) {
				if (visited.get(cAdjacent) == 0) {
					if (cAdjacent == p2) {
						answer = current.depth + 1;
						break;
					}
					// mark node as added - GREY
					visited.set(cAdjacent,1);
					Q.push_back(QueryBFS(cAdjacent, current.depth + 1));
					size++;
				}
			}
		} // end of neighbors processing
		  // check if an answer has been found
		if (answer != -1) {
			break;
		}
	}
	// no path found
	//Answers1.push_back(-1);
	//printf("q1: [%d]", answer);
	std::stringstream ss;
	ss << answer;
	Answers[qid] = ss.str();
}

long findTagLargestComponent(vector<Q2ListNode*> people, unsigned int queryBirth, long minComponentSize) {
	// make the persons for this graph a set
	long indexValidPersons=0;
	//LPBitset newGraphPersons(N_PERSONS);
	LPSparseBitset newGraphPersons;
	for( unsigned long i=0,sz=people.size(); i<sz && people[i]->birth >= queryBirth; i++ ){
		newGraphPersons.set(people[i]->personId);
		indexValidPersons++;
	}

	// check if we have enough people to make a larger component
	if( indexValidPersons < minComponentSize ){
		return 0;
	}

	// now we have to calculate the shortest paths between them
	LPSparseArrayGeneric<long> components;
	LPSparseArrayGeneric<char> visitedBFS;
	//LPBitset visitedBFS(N_PERSONS);
	vector<long> componentsIds;
	vector<long> Q;
	Q.reserve(128);
	long currentCluster = -1;
	for (long i = 0, sz = indexValidPersons; i < sz; i++) {
		//if( !visitedBFS.isSet(people[i]->personId) ){
		if( visitedBFS.get(people[i]->personId) == 0 ){
			currentCluster++;
			componentsIds.push_back(currentCluster);
			Q.clear();
			Q2ListNode *cPerson = people[i];
			long qIndex = 0;
			long qSize = 1;
			Q.push_back(cPerson->personId);
			while (qIndex < qSize) {
				long c = Q[qIndex];
				qIndex++;

				//if( visitedBFS.isSet(c) )
					//continue;
				//visitedBFS.set(c);
				visitedBFS.set(c, 2);

				++*(components.getRef(currentCluster));

				//printf("c[%ld] [%ld]\n", currentCluster, components[currentCluster] );

				// insert each unvisited neighbor of the current node
				long *edges = Persons[c].adjacentPersonsIds;
				for (int e = 0, szz = Persons[c].adjacents; e < szz; e++) {
					long eId = edges[e];
					//if ( newGraphPersons.isSet(eId) && !visitedBFS.isSet(eId)) {
					if ( newGraphPersons.isSet(eId) && visitedBFS.get(eId) == 0) {
						visitedBFS.set(eId, 1);
						//visitedBFS.set(eId);
						Q.push_back(eId);
						qSize++;
					}
				}
			}
			// end of BFS for unvisited person
		}
	}

	// find the maximum cluster
	long maxComponent = 0;
	for( long i=0,sz=componentsIds.size(); i<sz; i++ ){
		long c = components.get(componentsIds[i]);
		if( c >= maxComponent ){
			maxComponent = c;
		}
	}
	return maxComponent;
}

bool Q2ResultListPredicate(const Q2ResultNode &a, const Q2ResultNode &b) {
	if( a.people > b.people )
		return true;
	if( a.people < b.people )
		return false;
	return strcmp( TagIdToName[a.tagId], TagIdToName[b.tagId] ) <= 0;
}

void query2(int k, char *date, int date_sz, long qid) {
	//printf("query 2: k[%d] date[%*s] dateNum[%d]\n", k, date_sz, date, getDateAsInt(date, date_sz));

	unsigned int queryBirth = getDateAsInt(date, date_sz);
	long minComponentSize = 0;

	list<Q2ResultNode> results;

	int currentResults = 0;
	for (long i = 0, sz = TagSubFinals.size(); i < sz; i++) {
		vector<Q2ListNode*> &people = TagSubFinals[i]->people;
		long currentTagSize = people.size();

		// do not need to process further in other tags
		if (currentTagSize < minComponentSize) {
			break;
		}
		// check the max birth date of the list in order to avoid
		// checking a list when there are no valid people
		if (people[0]->birth < queryBirth) {
			continue;
		}

		// find the largest component for the current tag
		long largestTagComponent = findTagLargestComponent(people, queryBirth, minComponentSize);

		// we have to check if the current tag should be in the results
		if (currentResults < k) {
			// NOT NEEDED - initialized above
			// minComponentSize = 0;
			results.push_back(Q2ResultNode(TagSubFinals[i]->tagId, largestTagComponent));
			currentResults++;
			if (currentResults == k) {
				results.sort(Q2ResultListPredicate);
				minComponentSize = results.back().people;
			}
		} else {
			// we need to discard another result only if this tag has larger component than our minimum
			if (largestTagComponent >= results.back().people) {
				//char found = 0;
				for (list<Q2ResultNode>::iterator itPerson = results.begin(),
						end = results.end(); itPerson != end; itPerson++) {
					//if ( (*itPerson).people < largestTagComponent) {
					if ( (*itPerson).people < largestTagComponent
							|| (strcmp( TagIdToName[(*itPerson).tagId], TagIdToName[TagSubFinals[i]->tagId] ) >= 0  && (*itPerson).people == largestTagComponent )) {
						// insert here
						results.insert(itPerson, Q2ResultNode(TagSubFinals[i]->tagId,largestTagComponent));
						// discard the last one - min
						results.pop_back();
						// update the minimum component
						if (largestTagComponent < minComponentSize)
							minComponentSize = largestTagComponent;
						break;
					}
				}
			}					// end if this is a valid tag
		}					// end if we have more than k results
	} // end for each tag component

	// print the K ids from the sorted list - according to the tag names for ties
	//results.sort(Q2ResultListPredicate);
	std::stringstream ss;
	for( list<Q2ResultNode>::iterator end=results.end(), itTag=results.begin(); itTag != end; itTag++ ){
		ss << TagIdToName[(*itTag).tagId] << " ";
	}
	Answers[qid] = ss.str().c_str();
}

int BFS_query3(long idA, long idB, int h) {
	//char *visited = (char*) malloc(N_PERSONS);
	//memset(visited, 0, N_PERSONS);

	LPSparseArrayGeneric<char> visited;

	vector<QueryBFS> Q;
	long qIndex = 0;
	long qSize = 1;
	Q.push_back(QueryBFS(idA, 0));
	while (qIndex < qSize) {
		QueryBFS cPerson = Q[qIndex];
		qIndex++;

		// we have reached the hop limit of the query
		// so we have to exit since the person we want to reach cannot be found
		// in less than h-hops since he should have already be found
		// while pushing the neighbors below. The destination node should
		// never appear here since he will never be pushed into the Queue.
		if (cPerson.depth > h) {
			break;
		}
		// mark person BLACK - visited
		//visited[cPerson.person] = 2;
		visited.set(cPerson.person,2);

		long *neighbors = Persons[cPerson.person].adjacentPersonsIds;
		for (long i = 0, sz = Persons[cPerson.person].adjacents; i < sz; i++) {
			long cB = neighbors[i];
			// if person is not visited and not added yet
			//if (visited[cB] == 0) {
			if ( visited.get(cB) == 0) {
				// check if this is our person
				if (idB == cB) {
					//free(visited);
					return cPerson.depth + 1;
				}
				// mark person as GREY - added
				visited.set(cB,1);
				Q.push_back(QueryBFS(cB, cPerson.depth + 1));
				qSize++;
			}
		}
	}
	//free(visited);
	return INT_MAX;
}

void query3(int k, int h, char *name, int name_sz, long qid) {
	//printf("query3 k[%d] h[%d] name[%*s] name_sz[%d]\n", k, h, name_sz, name, name_sz);

	vector<long> persons;
	LPSparseBitset *visitedPersons = new LPSparseBitset();
	// get all the persons that are related to the place passed in
	char *visitedPlace = (char*) malloc(Places.size());
	memset(visitedPlace, 0, Places.size());
	TrieNode *place = TrieFind(PlacesToId, name, name_sz);
	long index = place->vIndex;
	vector<long> Q_places;
	Q_places.push_back(index);
	// set as added
	visitedPlace[index] = 1;
	long qIndex = 0;
	long qSize = 1;
	while (qIndex < qSize) {
		long cPlace = Q_places[qIndex];
		qIndex++;
		// set visited
		visitedPlace[cPlace] = 2;
		PlaceNodeStruct *cPlaceStruct = Places[cPlace];
		//std::copy (cPlaceStruct->personsThis.begin(),cPlaceStruct->personsThis.end(),std::back_inserter(persons));
		std::vector<long>::iterator cPerson = cPlaceStruct->personsThis.begin();
		std::vector<long>::iterator end = cPlaceStruct->personsThis.end();
		persons.reserve(persons.size() + (end - cPerson));
		for (; cPerson != end; cPerson++) {
			//if( visitedPersons[*cPerson] )
			if (visitedPersons->isSet(*cPerson))
				continue;
			visitedPersons->set(*cPerson);
			persons.push_back(*cPerson);
		}

		for (std::vector<long>::iterator it =
				cPlaceStruct->placesPartOfIndex.begin();
				it != cPlaceStruct->placesPartOfIndex.end(); ++it) {
			// if not visited
			if (visitedPlace[*it] == 0) {
				// set as added
				visitedPlace[*it] = 1;
				Q_places.push_back(*it);
				qSize++;
				//printf("added place[%ld] of [%ld]persons\n", *it, Places[*it]->personsThis.size() );
			}
		}
	}
	free(visitedPlace);
	delete visitedPersons;

	//printf("found for place [%*s] persons[%ld] index[%ld]\n", name_sz, name, persons.size(), index);

	// now we have all the required persons so we have to calculate the common tags
	// for each pair and insert them into the priority queue in order to get the maximum K later
	priority_queue<Query3PQ, vector<Query3PQ>, Query3PQ_Comparator> PQ;
	for (long i = 0, end = persons.size() - 1; i < end; ++i) {
		long idA = persons[i];
		//for( std::vector<long>::iterator idA = persons.begin(),end=persons.begin()+persons.size()-1; idA != end ; ++idA ){
		//for( std::vector<long>::iterator idB = idA+1; idB != persons.end(); ++idB ){
		for (long j = i + 1, endd = persons.size(); j < endd; ++j) {
			long idB = persons[j];
			// WE DO NOT HAVE TO CHECK THESE PEOPLE IF THEY ARE NOT IN THE SAME SUBGRAPH
			if (Persons[idA].subgraphNumber != Persons[idB].subgraphNumber)
				continue;
			// we now have to calculate the common tags between these two people
			int cTags = 0;
			vector<long> &tagsA = PersonToTags[idA].tags;
			vector<long> &tagsB = PersonToTags[idB].tags;
			std::vector<long>::const_iterator iA = tagsA.begin();
			std::vector<long>::const_iterator endA = tagsA.end();
			std::vector<long>::const_iterator iB = tagsB.begin();
			std::vector<long>::const_iterator endB = tagsB.end();
			for (; iA != endA && iB != endB;) {
				if (*iA < *iB)
					iA++;
				else if (*iB < *iA)
					iB++;
				else if (*iA == *iB) {
					cTags++;
					iA++;
					iB++;
				}
			}
			//printf("idA[%ld] idB[%ld] common[%ld]\n", idA, idB, cTags);
			if (idA <= idB) {
				PQ.push(Query3PQ(idA, idB, cTags));
			} else {
				PQ.push(Query3PQ(idB, idA, cTags));
			}
		}
	}

	// now we have to pop the K most common tag pairs
	// but we also have to check that the distance between them
	// is below the H-hops needed by the query.
	std::stringstream ss;
	for (; k > 0; k--) {
		if( PQ.empty() )
			break;
		long idA = -1;
		long idB = -1;
		//long cTags = -1;
		while (!PQ.empty()) {
			const Query3PQ &cPair = PQ.top();
			idA = cPair.idA;
			idB = cPair.idB;
			//cTags = cPair.commonTags;
			PQ.pop();
			int distance = BFS_query3(idA, idB, h);
			if (distance <= h) {
				// we have an answer so exit the while
				ss << idA << "|" << idB << " ";
				break;
			}
		}
		//ss << idA << "|" << idB << "[" << cTags << "] ";
	}
	//Answers3.push_back(ss.str());
	//printf("q3: [%s]\n", ss.str().c_str());
	Answers[qid] = ss.str();
}

void query4(int k, char *tag, int tag_sz, long qid) {
	//printf("query 4: k[%d] tag[%*s]\n", k, tag_sz, tag);

	long tagIndex = TrieFind(TagToIndex, tag, tag_sz)->vIndex;
	vector<Query4PersonStruct> persons;
	vector<long> &forums = Tags[tagIndex]->forums;
	// TODO - consider having SET here for space issues - and also in query 3
	//LPBitset *visitedPersons = new LPBitset(N_PERSONS);
	LPSparseBitset *visitedPersons = new LPSparseBitset();
	for (int cForum = 0, fsz = forums.size(); cForum < fsz; cForum++) {
		vector<long> &cPersons = Forums[forums[cForum]];
		for (int cPerson = 0, psz = cPersons.size(); cPerson < psz; cPerson++) {
			long personId = cPersons[cPerson];
			//if( (*visitedPersons)[personId] )
			if (visitedPersons->isSet(personId))
				continue;
			visitedPersons->set(personId);
			persons.push_back(Query4PersonStruct(personId, 0, 0, 0.0));
		}
	}

	// now I want to create a new graph containing only the required edges
	// to speed up the shortest paths between all of them
	//LPSparseArrayGeneric<vector<long> > newGraph;
	MAP_INT_VecL newGraph;
	for (int i = 0, sz = persons.size(); i < sz; i++) {
		long pId = persons[i].person;
		long *edges = Persons[pId].adjacentPersonsIds;
		vector<long> &newEdges = newGraph[pId];
		//vector<long> *newEdges = newGraph.getRef(pId);
		for (int j = 0, szz = Persons[pId].adjacents; j < szz; j++) {
			if (visitedPersons->isSet(edges[j])) {
				//newEdges->push_back(edges[j]);
				newEdges.push_back(edges[j]);
			}
		}
	}
	// safe to delete the visitedPersons since we got the people for this tag
	delete visitedPersons;


	// now we have to calculate the shortest paths between them
	int n_1 = persons.size() - 1;
	//MAP_INT_INT visitedBFS;
	LPSparseArrayGeneric<char> visitedBFS;
	vector<QueryBFS> Q;
	for (int i = 0, sz = persons.size(); i < sz; i++) {
		visitedBFS.clear();
		Q.clear();
		Query4PersonStruct &cPerson = persons[i];
		long qIndex = 0;
		long qSize = 1;
		Q.push_back(QueryBFS(cPerson.person, 0));
		while (qIndex < qSize) {
			QueryBFS &c = Q[qIndex];
			qIndex++;
			//visitedBFS[c.person] = 2;
			visitedBFS.set(c.person, 2);
			// update info for the current person centrality
			cPerson.s_p += c.depth;

			// insert each unvisited neighbor of the current node
			vector<long> &edges = newGraph[c.person];
			//vector<long> *edges = newGraph.getRef(c.person);
			//long*edges = Persons[c.person].adjacentPersonsIds;
			for (int e = 0, szz = edges.size(); e < szz; e++) {
			//for (int e = 0, szz = edges->size(); e < szz; e++) {
			//for (int e = 0, szz = Persons[c.person].adjacents; e < szz; e++) {
				//long eId = (*edges)[e];
				long eId = edges[e];
				//if (visitedBFS[eId] == 0) {
				if ( visitedBFS.get(eId) == 0) {
				//if ( visitedPersons->isSet(eId) && visitedBFS.get(eId) == 0) {
					//visitedBFS[eId] = 1;
					visitedBFS.set(eId, 1);
					Q.push_back(QueryBFS(eId, c.depth + 1));
					qSize++;
				}
			}
		}
		// we do not have to check if n_1 == 0 since if it was the outer FOR here would not execute
		if (cPerson.s_p == 0)
			cPerson.centrality = 0;
		else {
			// calculate the centrality for this person
			cPerson.r_p += qSize - 1;
			cPerson.centrality = ((double) (cPerson.r_p * cPerson.r_p))
					/ (n_1 * cPerson.s_p);
		}
	}

	//delete visitedPersons;

	// we now just have to return the K persons with the highest centrality
	std::stable_sort(persons.begin(), persons.end(),Query4PersonStructPredicate);
	std::stringstream ss;
	for (int i = 0; i < k; i++) {
		//ss << persons[i].person << ":" << persons[i].centrality << " ";
		ss << persons[i].person << " ";
	}
	//Answers4.push_back(ss.str());
	//printf("%s\n", ss.str().c_str());
	Answers[qid] = ss.str();
}

//////////////////////////////////////////////////////////////
//////////////////////// WORKER JOBS /////////////////////////

struct Query1WorkerStruct {
	int p1;
	int p2;
	int x;
	long qid;
};

struct Q1Worker{
	int start;
	int end;
};

vector<Query1WorkerStruct*> Query1Structs;

void* Query1WorkerFunction(void *args) {
	Q1Worker *qws = (Q1Worker*)args;
	Query1WorkerStruct *currentJob;

	for( int i=qws->start, end=qws->end; i<end; i++ ){
		currentJob = Query1Structs[i];
		query1(currentJob->p1, currentJob->p2, currentJob->x, currentJob->qid);
		free(currentJob);
		// the following can be omitted for speedups
		Query1Structs[i] = 0;
	}

	free(qws);
	// end of job
	return 0;
}

void executeQuery1Jobs(int q1threads){
	int totalJobs = Query1Structs.size();
	int perThreadJobs = totalJobs / q1threads;
	int untilThreadJobsPlus = totalJobs % q1threads;
	int lastEnd = 0;
	pthread_t *worker_threads = (pthread_t*)malloc(sizeof(pthread_t)*q1threads);
	cpu_set_t mask;
	for (int i = 0; i < q1threads; i++) {
		Q1Worker *qws = (Q1Worker*)malloc(sizeof(Q1Worker));
		qws->start = lastEnd;
		if( i < untilThreadJobsPlus ){
			lastEnd += perThreadJobs + 1;
		}else{
			lastEnd += perThreadJobs;
		}
		qws->end = lastEnd;
		pthread_create(&worker_threads[i], NULL,reinterpret_cast<void* (*)(void*)>(Query1WorkerFunction), qws );
		//fprintf( stderr, "[%ld] thread[%d] added\n", worker_threads[i], i );
		CPU_ZERO(&mask);
		CPU_SET( ((i+1) % NUM_CORES) , &mask);
		if (pthread_setaffinity_np(worker_threads[i], sizeof(cpu_set_t), &mask) != 0) {
			fprintf(stderr, "Error setting thread affinity for tid[%d][%d]\n", i, ((i+1) % NUM_CORES));
		}else{
			//fprintf(stderr, "Successfully set thread affinity for tid[%d][%d]\n", i, ((i+1) % NUM_CORES));
		}
	}
}

/*
void* Query1WorkerFunction(int tid, void *args) {
	Query1WorkerStruct *qArgs = (Query1WorkerStruct*) args;
	//printf("tid[%d] [%d]\n", tid, qArgs->qid);
	query1(qArgs->p1, qArgs->p2, qArgs->x, qArgs->qid);

	free(qArgs);
	// end of job
	return 0;
}
*/

struct Query2WorkerStruct {
	int k;
	char *date;
	int date_sz;
	long qid;
};
void* Query2WorkerFunction(int tid, void *args) {
	Query2WorkerStruct *qArgs = (Query2WorkerStruct*) args;
	//printf("tid[%d] [%d]\n", tid, *(int*)args);
	query2(qArgs->k, qArgs->date, qArgs->date_sz, qArgs->qid);

	free(qArgs->date);
	free(qArgs);
	// end of job
	return 0;
}

struct Query3WorkerStruct {
	int k;
	int h;
	char *name;
	int name_sz;
	long qid;
};
void* Query3WorkerFunction(int tid, void *args) {
	Query3WorkerStruct *qArgs = (Query3WorkerStruct*) args;
	//printf("tid[%d] [%d]\n", tid, *(int*)args);
	query3(qArgs->k, qArgs->h, qArgs->name, qArgs->name_sz, qArgs->qid);

	free(qArgs->name);
	free(qArgs);
	// end of job
	return 0;
}

struct Query4WorkerStruct {
	int k;
	char *tag;
	int tag_sz;
	long qid;
};
void* Query4WorkerFunction(int tid, void *args) {
	Query4WorkerStruct *qArgs = (Query4WorkerStruct*) args;
	//printf("tid[%d] [%d]\n", tid, *(int*)args);
	query4(qArgs->k, qArgs->tag, qArgs->tag_sz, qArgs->qid);

	free(qArgs->tag);
	free(qArgs);
	// end of job
	return 0;
}

struct QueryWorkerStruct {
	void** query_worker_struct;
	char query_type;
	int numOfJobs;
};

void* QueryWorkerFunction(int tid, void *args) {
	QueryWorkerStruct* qworker = (QueryWorkerStruct*) args;
	switch (qworker->query_type) {
	case 1:
		for (int i = 0, sz = qworker->numOfJobs; i < sz; i++) {
			//Query1WorkerFunction(tid, qworker->query_worker_struct[i]);
			Query1WorkerStruct *qArgs = (Query1WorkerStruct*) qworker->query_worker_struct[i];
			query1(qArgs->p1, qArgs->p2, qArgs->x, qArgs->qid);
			free(qArgs);
		}
		break;
	case 2:
		for (int i = 0, sz = qworker->numOfJobs; i < sz; i++) {
			//Query2WorkerFunction(tid, qworker->query_worker_struct[i]);
			Query2WorkerStruct *qArgs = (Query2WorkerStruct*) qworker->query_worker_struct[i];
			query2(qArgs->k, qArgs->date, qArgs->date_sz, qArgs->qid);
			free(qArgs);
		}
		break;
	case 3:
		for (int i = 0, sz = qworker->numOfJobs; i < sz; i++) {
			//Query3WorkerFunction(tid, qworker->query_worker_struct[i]);
			Query3WorkerStruct *qArgs = (Query3WorkerStruct*) qworker->query_worker_struct[i];
			query3(qArgs->k, qArgs->h, qArgs->name, qArgs->name_sz, qArgs->qid);
			free(qArgs->name);
			free(qArgs);
		}
		break;
	case 4:
		for (int i = 0, sz = qworker->numOfJobs; i < sz; i++) {
			//Query4WorkerFunction(tid, qworker->query_worker_struct[i]);
			Query4WorkerStruct *qArgs = (Query4WorkerStruct*) qworker->query_worker_struct[i];
			query4(qArgs->k, qArgs->tag, qArgs->tag_sz, qArgs->qid);
			free(qArgs->tag);
			free(qArgs);
		}
		break;
	}
	free(qworker->query_worker_struct);
	free(qworker);
	// end of job
	return 0;
}

//////////////////////////////////////////////////////////////

///////////////////////////////////////////////////////////////////////
// MAIN PROGRAM
///////////////////////////////////////////////////////////////////////

void _initializations() {
	CommentToPerson = new MAP_INT_INT();
	PlaceIdToIndex = new MAP_INT_INT();
	OrgToPlace = new MAP_INT_INT();

	PlacesToId = TrieNode_Constructor();
	Places.reserve(2048);

	TagToIndex = TrieNode_Constructor();
	Tags.reserve(2048);
	TagIdToIndex = new MAP_INT_INT();

}

void _destructor() {
	delete[] Persons;
	delete[] PersonToTags;
	TrieNode_Destructor(PlacesToId);
	TrieNode_Destructor(TagToIndex);
}

void addPoolJobs(char prevQType, vector<Query1WorkerStruct*> &vec1,
		vector<Query2WorkerStruct*> &vec2, vector<Query3WorkerStruct*> &vec3,
		vector<Query4WorkerStruct*> &vec4) {
	switch (prevQType) {
	case 1: {
		long totalJobsAssigned = 0, totalJobs = vec1.size();
		int i;
		while (totalJobsAssigned < totalJobs) {
			QueryWorkerStruct* qws = (QueryWorkerStruct*) malloc(
					sizeof(QueryWorkerStruct));
			qws->query_type = 1;
			qws->query_worker_struct = (void**) malloc(
					sizeof(Query1WorkerStruct*) * BATCH_Q1);
			for (i = 0; i < BATCH_Q1 && totalJobsAssigned < totalJobs; i++) {
				qws->query_worker_struct[i] = vec1[totalJobsAssigned];
				totalJobsAssigned++;
			}
			qws->numOfJobs = i;
			//lp_threadpool_addjob_nolock(threadpool,reinterpret_cast<void* (*)(int,void*)>(QueryWorkerFunction), (void*)qws );
			lp_threadpool_addjob(threadpool,reinterpret_cast<void* (*)(int,void*)>(QueryWorkerFunction), (void*)qws );
		}
		vec1.clear();
	}
		break;
	case 2:

		break;
	case 3: {
		long totalJobsAssigned = 0, totalJobs = vec3.size();
		int i;
		while (totalJobsAssigned < totalJobs) {
			QueryWorkerStruct* qws = (QueryWorkerStruct*) malloc(
					sizeof(QueryWorkerStruct));
			qws->query_type = 3;
			qws->query_worker_struct = (void**) malloc(
					sizeof(Query3WorkerStruct*) * BATCH_Q3);
			for (i = 0; i < BATCH_Q3 && totalJobsAssigned < totalJobs; i++) {
				qws->query_worker_struct[i] = vec3[totalJobsAssigned];
				totalJobsAssigned++;
			}
			qws->numOfJobs = i;
			lp_threadpool_addjob_nolock(threadpool,
					reinterpret_cast<void* (*)(int,
							void*)>(QueryWorkerFunction), (void*)qws );
		}
		vec3.clear();
	}
		break;
	case 4: {
		long totalJobsAssigned = 0, totalJobs = vec4.size();
		int i;
		while (totalJobsAssigned < totalJobs) {
			QueryWorkerStruct* qws = (QueryWorkerStruct*) malloc(
					sizeof(QueryWorkerStruct));
			qws->query_type = 4;
			qws->query_worker_struct = (void**) malloc(
					sizeof(Query4WorkerStruct*) * BATCH_Q4);
			for (i = 0; i < BATCH_Q4 && totalJobsAssigned < totalJobs; i++) {
				qws->query_worker_struct[i] = vec4[totalJobsAssigned];
				totalJobsAssigned++;
			}
			qws->numOfJobs = i;
			lp_threadpool_addjob_nolock(threadpool,
					reinterpret_cast<void* (*)(int,
							void*)>(QueryWorkerFunction), (void*)qws );
		}
		vec4.clear();
	}
		break;
	}
}

void readQueries(char *queriesFile) {
	///////////////////////////////////////////////////////////////////
	// READ THE QUERIES
	///////////////////////////////////////////////////////////////////
	char path[1024];
	path[0] = '\0';
	strcat(path, queriesFile);

	// COUNT THE NUMBER OF QUERIES
	N_QUERIES = countFileLines(path);
	Answers.resize(N_QUERIES);

	// initialize the vars for the job assignments
	Query1Structs.reserve(2048);

	vector<Query2WorkerStruct*> vec2;
	//vec2.reserve(BATCH_Q2 << 1);
	vector<Query3WorkerStruct*> vec3;
	//vec3.reserve(BATCH_Q3 << 1);
	vector<Query4WorkerStruct*> vec4;
	//vec4.reserve(BATCH_Q4 << 1);

	char prevQType = -1;
	long qid = 0;

	FILE *input = fopen(path, "r");
	if (input == NULL) {
		printErr("could not open queries file!");
	}
	long lSize;
	char *buffer = getFileBytes(input, &lSize);

	char *startLine = buffer;
	char *EndOfFile = buffer + lSize;
	char *lineEnd;
	while (startLine < EndOfFile) {
		lineEnd = (char*) memchr(startLine, '\n', LONGEST_LINE_READING);
		int queryType = atoi(startLine + 5);

		// handle the new query
		switch (queryType) {
		case 1: {
			char *second = ((char*) memchr(startLine + 7, ',', LONGEST_LINE_READING)) + 1;
			*(second - 1) = '\0';
			char *third = ((char*) memchr(second, ',', LONGEST_LINE_READING)) + 1;
			*(lineEnd - 1) = '\0';
			//query1(atoi(startLine+7), atoi(second), atoi(third), qid);

			Query1WorkerStruct *qwstruct = (Query1WorkerStruct*) malloc(
					sizeof(Query1WorkerStruct));
			qwstruct->p1 = atol(startLine + 7);
			qwstruct->p2 = atol(second);
			qwstruct->x = atol(third);
			qwstruct->qid = qid;
			Query1Structs.push_back(qwstruct);
			//lp_threadpool_addjob_nolock(threadpool,reinterpret_cast<void* (*)(int,void*)>(Query1WorkerFunction), (void*)qwstruct );

			break;
		}
		case 2: {
			char *second = ((char*) memchr(startLine + 7, ',', LONGEST_LINE_READING)) + 1;
			*(second - 1) = '\0';
			*(lineEnd - 1) = '\0';
			char *date = second + 1; // to skip one space
			//query2(atoi(startLine + 7), date, lineEnd - 1 - date, qid);

			Query2WorkerStruct *qwstruct = (Query2WorkerStruct*) malloc(sizeof(Query2WorkerStruct));
			qwstruct->k = atoi(startLine + 7);
			qwstruct->date_sz = lineEnd-1-date;
			qwstruct->date = strndup(date, qwstruct->date_sz);
			qwstruct->qid = qid;
			//vec2.push_back(qwstruct);
			lp_threadpool_addjob_nolock(threadpool,reinterpret_cast<void* (*)(int,void*)>(Query2WorkerFunction), (void*)qwstruct );
			//lp_threadpool_addjob(threadpool,reinterpret_cast<void* (*)(int,void*)>(Query2WorkerFunction), (void*)qwstruct );

			break;
		}
		case 3: {
			char *second = ((char*) memchr(startLine + 7, ',', LONGEST_LINE_READING)) + 1;
			*(second - 1) = '\0';
			char *third = ((char*) memchr(second, ',', LONGEST_LINE_READING)) + 1;
			*(third - 1) = '\0';
			*(lineEnd - 1) = '\0';
			char *name = third + 1; // to skip one space
			int name_sz = lineEnd - 1 - name;
			//query3(atoi(startLine + 7), atoi(second), name, name_sz, qid);

			char *placeName = (char*) malloc(name_sz + 1);
			strncpy(placeName, name, name_sz + 1);
			Query3WorkerStruct *qwstruct = (Query3WorkerStruct*) malloc(
					sizeof(Query3WorkerStruct));
			qwstruct->k = atoi(startLine + 7);
			qwstruct->h = atoi(second);
			qwstruct->name = placeName;
			qwstruct->name_sz = name_sz;
			qwstruct->qid = qid;
//			vec3.push_back(qwstruct);
			lp_threadpool_addjob_nolock(threadpool,reinterpret_cast<void* (*)(int,void*)>(Query3WorkerFunction), qwstruct );
			//lp_threadpool_addjob(threadpool,reinterpret_cast<void* (*)(int,void*)>(Query3WorkerFunction), qwstruct );

			break;
		}
		case 4: {
			char *second = ((char*) memchr(startLine + 7, ',', LONGEST_LINE_READING)) + 1;
			*(second - 1) = '\0';
			*(lineEnd - 1) = '\0';
			char *name = second + 1; // to skip one space
			int tag_sz = lineEnd - 1 - name;
			//query4(atoi(startLine + 7), name, tag_sz, qid);

			char *tagName = (char*) malloc(tag_sz + 1);
			strncpy(tagName, name, tag_sz + 1);
			Query4WorkerStruct *qwstruct = (Query4WorkerStruct*) malloc(
					sizeof(Query4WorkerStruct));
			qwstruct->k = atoi(startLine + 7);
			qwstruct->tag = tagName;
			qwstruct->tag_sz = tag_sz;
			qwstruct->qid = qid;
//			vec4.push_back(qwstruct);
			lp_threadpool_addjob_nolock(threadpool,reinterpret_cast<void* (*)(int,void*)>(Query4WorkerFunction), qwstruct );
			//lp_threadpool_addjob(threadpool,reinterpret_cast<void* (*)(int,void*)>(Query4WorkerFunction), qwstruct );

			break;
		}
		default: {
			*lineEnd = '\0';
			//printOut(startLine);
		}
		}
		startLine = lineEnd + 1;
		prevQType = queryType;
		qid++;
	}
	free(buffer);

	//addPoolJobs(prevQType, vec1, vec2, vec3, vec4);
}

int main(int argc, char** argv) {

	inputDir = argv[1];
	queryFile = argv[2];

	// make the master thread to run only on the 1st core
	cpu_set_t mask;
	CPU_ZERO(&mask);
	CPU_SET( 0 , &mask);


	// MAKE GLOBAL INITIALIZATIONS
	char msg[100];
	_initializations();

	long long time_global_start = getTime();

	// Initialize the threadpool
	/*
	threadpool = lp_threadpool_init( WORKER_THREADS, NUM_CORES);
	lp_threadpool_addjob_nolock(threadpool,reinterpret_cast<void* (*)(int,void*)>(phase1_ReadPersons), NULL );
	lp_threadpool_addjob_nolock(threadpool,reinterpret_cast<void* (*)(int,void*)>(phase1_ReadPlacesFiles), NULL );
	lp_threadpool_startjobs(threadpool);
	synchronize_complete(threadpool);
	*/

	threadpool = lp_threadpool_init( WORKER_THREADS, NUM_CORES);
	readQueries(queryFile);

#ifdef DEBUGGING
	long time_queries_end = getTime();
	sprintf(msg, "queries file time: %ld", time_queries_end - time_global_start);
	printOut(msg);
#endif

	/////////////////////////////////
	readPersons(inputDir);
	readPersonKnowsPerson(inputDir);

#ifdef DEBUGGING
	long time_persons_end = getTime();
	sprintf(msg, "persons graph time: %ld", time_persons_end - time_global_start);
	printOut(msg);
#endif

	readComments(inputDir);

	///////////////////////////////////////////////////////////////////
	// PROCESS THE COMMENTS OF EACH PERSON A
	// - SORT THE EDGES BASED ON THE COMMENTS from A -> B
	///////////////////////////////////////////////////////////////////
	postProcessComments();

	// now we can start executing QUERY 1 - we use WORKER_THREADS - 1
	executeQuery1Jobs(WORKER_THREADS - 1);

#ifdef DEBUGGING
	long time_comments_end = getTime();
	sprintf(msg, "comments process time: %ld", time_comments_end - time_global_start);
	printOut(msg);
#endif

	readPlaces(inputDir);
	readPlacePartOfPlace(inputDir);
	readPersonLocatedAtPlace(inputDir);
	readOrgsLocatedAtPlace(inputDir);
	readPersonWorksStudyAtOrg(inputDir);

	delete PlaceIdToIndex;
	PlaceIdToIndex = NULL;

#ifdef DEBUGGING
	long time_places_end = getTime();
	sprintf(msg, "places process time: %ld", time_places_end - time_global_start);
	printOut(msg);
#endif

	// Q3
	readPersonHasInterestTag(inputDir);
	postProcessTagBirthdays();

	// Q4
	readTags(inputDir);
	readForumHasTag(inputDir);
	readForumHasMember(inputDir);

#ifdef DEBUGGING
	long time_tags_end = getTime();
	sprintf(msg, "tags process time: %ld", time_tags_end - time_global_start);
	printOut(msg);
#endif


	lp_threadpool_startjobs(threadpool);
	// start workers
	//lp_threadpool_startjobs(threadpool);
	synchronize_complete(threadpool);

#ifdef DEBUGGING
	long time_queries_end = getTime();
	sprintf(msg, "queries process time: %ld", time_queries_end - time_global_start);
	printOut(msg);

	/////////////////////////////////
	long long time_global_end = getTime();
	sprintf(msg, "\nTotal time: micros[%lld] seconds[%.6f]",
			time_global_end - time_global_start,
			(time_global_end - time_global_start) / 1000000.0);
	printOut(msg);

#endif

	for (long i = 0, sz = Answers.size(); i < sz; i++) {
		//printf("answer %d: %d\n", i, Answers1[i]);
		printf("%s\n", Answers[i].c_str());
	}

	long long time_global_end = getTime();
	sprintf(msg, "\nTotal time: micros[%lld] seconds[%.6f]\n",
			time_global_end - time_global_start,
			(time_global_end - time_global_start) / 1000000.0);
	printErr(msg);


	// destroy the remaining indexes
	_destructor();
}

////////////////////////////////////////////////////////////////////////
// TRIE IMPLEMENTATION
////////////////////////////////////////////////////////////////////////
TrieNode* TrieNode_Constructor() {
	TrieNode* n = (TrieNode*) malloc(sizeof(TrieNode));
	if (!n)
		printErr("error allocating TrieNode");
	n->valid = 0;
	memset(n->children, 0, VALID_PLACE_CHARS * sizeof(TrieNode*));
	return n;
}
void TrieNode_Destructor(TrieNode* node) {
	for (int i = 0; i < VALID_PLACE_CHARS; i++) {
		if (node->children[i] != 0) {
			TrieNode_Destructor(node->children[i]);
		}
	}
	free(node);
}
TrieNode* TrieInsert(TrieNode* node, const char* name, char name_sz, long id,
		long index) {
	int ptr = 0;
	int pos;
	while (ptr < name_sz) {
		//pos=name[ptr]-'a';
		pos = (unsigned char) name[ptr];
		if (node->children[pos] == 0) {
			node->children[pos] = TrieNode_Constructor();
		}
		node = node->children[pos];
		ptr++;
	}
	// if already exists we do not overwrite but just return the existing one
	if (1 == node->valid) {
		return node;
	}
	node->valid = 1;
	node->realId = id;
	node->vIndex = index;
	return node;
}
TrieNode* TrieFind(TrieNode* root, const char* name, char name_sz) {
	int p, i, found = 1;
	for (p = 0; p < name_sz; p++) {
		//i = word[p] -'a';
		i = (unsigned char) name[p];
		if (root->children[i] != 0) {
			root = root->children[i];
		} else {
			found = 0;
			break;
		}
	}
	if (found && root->valid) {
		// WE HAVE A MATCH SO return the node
		return root;
	}
	return 0;
}

