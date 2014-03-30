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
#include <deque>
#include <vector>
#include <queue>
#include <algorithm>
#include <iterator>
#include <sstream>

#include <tr1/unordered_map>
#include <tr1/unordered_set>

#include "lplibs/LPBitset.h"
#include "lplibs/LPThreadpool.h"
#include "lplibs/LPSparseBitset.h"
#include "lplibs/LPSparseArrayGeneric.h"
#include "lplibs/atomic_ops_if.h"

using namespace std;
using std::tr1::unordered_map;
using std::tr1::unordered_set;
using std::tr1::hash;

//#define DEBUGGING 1
#define FILE_VBUF_SIZE 1<<20
#define FILE_BUFFER_SIZE 1<<20

#define CACHE_LINE_SIZE 64

#define VALID_PLACE_CHARS 256
#define LONGEST_LINE_READING 2048

#define NUM_CORES 4
#define Q4_JOB_WORKERS 2
#define Q4_THREADPOOOL_THREADS 4
#define Q3_THREADPOOOL_THREADS 4
#define Q1_WORKER_THREADS NUM_CORES
#define Q2_WORKER_THREADS NUM_CORES

#define COMMENTS_WORKERS NUM_CORES

#define NUM_THREADS WORKER_THREADS+1

int isLarge = 0;

///////////////////////////////////////////////////////////////////////////////
// structs
///////////////////////////////////////////////////////////////////////////////

//typedef map<int, int> MAP_INT_INT;
typedef std::tr1::unordered_map<int, int, hash<int> > MAP_INT_INT;
typedef std::tr1::unordered_map<long, char, hash<long> > MAP_LONG_CHAR;
typedef std::tr1::unordered_map<long, int, hash<long> > MAP_LONG_INT;
typedef std::tr1::unordered_map<long, long, hash<long> > MAP_LONG_LONG;
typedef std::tr1::unordered_map<int, vector<long>, hash<int> > MAP_INT_VecL;
typedef std::tr1::unordered_map<long, char*, hash<long> > MAP_LONG_STRING;

#include "sparsehash/dense_hash_map.h"
#include "sparsehash/sparse_hash_map.h"
using GOOGLE_NAMESPACE::dense_hash_map;
using GOOGLE_NAMESPACE::sparse_hash_map;

// A version of each of the hashtable classes we test, that has been
// augumented to provide a common interface.  For instance, the
// sparse_hash_map and dense_hash_map versions set empty-key and
// deleted-key (we can do this because all our tests use int-like
// keys), so the users don't have to.  The hash_map version adds
// resize(), so users can just call resize() for all tests without
// worrying about whether the map-type supports it or not.

template<typename K, typename V, typename H>
class EasyUseSparseHashMap : public sparse_hash_map<K,V,H> {
 public:
  EasyUseSparseHashMap() {
    this->set_deleted_key(-1);
  }
};

template<typename K, typename V>
class EasySparseHashMap : public sparse_hash_map<K,V> {
 public:
  EasySparseHashMap() {
    this->set_deleted_key(-1);
  }
};

template<typename K, typename V, typename H>
class EasyUseDenseHashMap : public dense_hash_map<K,V,H> {
 public:
  EasyUseDenseHashMap() {
    this->set_empty_key(-1);
    this->set_deleted_key(-2);
  }
};

template<typename K, typename V>
class EasyDenseHashMap : public dense_hash_map<K,V> {
 public:
  EasyDenseHashMap() {
    this->set_empty_key(-1);
    this->set_deleted_key(-2);
  }
};

// For pointers, we only set the empty key.
template<typename K, typename V, typename H>
class EasyUseSparseHashMap<K*, V, H> : public sparse_hash_map<K*,V,H> {
 public:
  EasyUseSparseHashMap() { }
};

template<typename K, typename V, typename H>
class EasyUseDenseHashMap<K*, V, H> : public dense_hash_map<K*,V,H> {
 public:
  EasyUseDenseHashMap() {
    this->set_empty_key((K*)(~0));
  }
};

// Google's HashMap
typedef EasyDenseHashMap<long,long> DMAP_LONG_LONG;
typedef EasyDenseHashMap<int,int> DMAP_INT_INT;
typedef EasySparseHashMap<long,long> SMAP_LONG_LONG;

// TODO - THE HASHMAP THAT WILL BE USED BELOW IN THE CODE
typedef MAP_LONG_LONG FINAL_MAP_LONG_LONG;
typedef MAP_INT_INT FINAL_MAP_INT_INT;

struct PersonStruct {
	PersonStruct() {
		subgraphNumber = -1;
		adjacents = 0;
		adjacentPersonsIds = NULL;
		adjacentPersonWeightsSorted = NULL;
	}
	long *adjacentPersonsIds;
	long adjacents;

	int *adjacentPersonWeightsSorted;

	int subgraphNumber;
}__attribute__((aligned(CACHE_LINE_SIZE)));
// Aligned for cache lines;

struct TrieNode {
	long realId;
	long vIndex;
	TrieNode* children[VALID_PLACE_CHARS];
};
// Aligned for cache lines;

struct CommentTrieNode {
	long personId;
	CommentTrieNode* children[10];
};
// Aligned for cache lines;

struct PlaceNodeStruct {
	long id;
	vector<long> personsThis;
	vector<long> placesPartOfIndex;
};
// Aligned for cache lines;

struct PersonTags {
	vector<long> tags;
};
// Aligned for cache lines;

struct TagNode {
	long id;
	TrieNode *tagNode;
	vector<long> forums;
};
// Aligned for cache lines;

// final sorted lists
struct Q2ListNode {
	Q2ListNode(long pId, unsigned int b){
		personId = pId;
		birth = b;
	}
	long personId;
	unsigned int birth;
};

// intermediate tree map
struct TagSubStruct {
	long tagId;
	long subId;
	vector<Q2ListNode> people;
};

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

bool Query3PQ_ComparatorStatic(const Query3PQ &left, long rightIdA, long rightIdB, int rightCommonTags) {
	if (left.commonTags > rightCommonTags)
		return false;
	if (left.commonTags < rightCommonTags)
		return true;
	if (left.idA < rightIdA)
		return false;
	if (left.idA > rightIdA)
		return true;
	if (left.idB <= rightIdB)
		return false;
	return true;
}
bool Query3PQ_ComparatorMinStatic(const Query3PQ &left, long rightIdA, long rightIdB, int rightCommonTags) {
	return !Query3PQ_ComparatorStatic(left, rightIdA, rightIdB, rightCommonTags);
}
class Query3PQ_ComparatorMin {
public:
	bool operator()(const Query3PQ &left, const Query3PQ &right) {
		return Query3PQ_ComparatorMinStatic(left, right.idA, right.idB, right.commonTags);
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

bool Q2ListNodePredicate(const Q2ListNode &a, const Q2ListNode &b) {
	return a.birth >= b.birth;
}

bool Q2ListPredicate(TagSubStruct* a, TagSubStruct* b) {
	return a->people.size() >= b->people.size();
}



///////////////////////////////////////////////////////////////////////////////
// FUNCTION PROTOTYPES
///////////////////////////////////////////////////////////////////////////////

TrieNode* TrieNode_Constructor();
void TrieNode_Destructor(TrieNode* node);
TrieNode* TrieInsert(TrieNode* node, const char* name, char name_sz, long id, long index);
TrieNode* TrieFind(TrieNode* root, const char* name, char name_sz);

CommentTrieNode* CommentTrieNode_Constructor();
void CommentTrieNode_Destructor(CommentTrieNode* node);
CommentTrieNode* CommentTrieInsert(CommentTrieNode* node, const char* commentId, char id_sz, long personId);
CommentTrieNode* CommentTrieFind(CommentTrieNode* root, const char* commentId, char id_sz);

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

long N_PERSONS = 0;
long N_TAGS = 0;
long N_SUBGRAPHS = 0;
long N_QUERIES = 0;

lp_threadpool *threadpool3;
lp_threadpool *threadpool4;

PersonStruct *Persons;
TrieNode *PlacesToId;
vector<PlaceNodeStruct*> Places;
PersonTags *PersonToTags;

vector<TagNode*> Tags;
TrieNode *TagToIndex; // required by Q4
MAP_LONG_STRING TagIdToName;

MAP_INT_VecL Forums;

vector<string> Answers;

// the structures below are only used as intermediate steps while
// reading the comments files. DO NOT USE THEM ANYWHERE
FINAL_MAP_LONG_LONG *CommentsPersonToPerson;
FINAL_MAP_INT_INT *CommentToPerson;

FINAL_MAP_INT_INT *PlaceIdToIndex;
FINAL_MAP_INT_INT *OrgToPlace;
FINAL_MAP_INT_INT *TagIdToIndex;

std::tr1::unordered_set<long> *Query4Tags;
std::tr1::unordered_set<long> *Query4TagForums;

// TODO
int *PersonBirthdays;
typedef std::tr1::unordered_map<unsigned long, TagSubStruct*, hash<unsigned long> > MAP_LONG_TSPTR;
//typedef std::map<unsigned long, TagSubStruct*> MAP_LONG_TSPTR;
MAP_LONG_TSPTR *TagSubBirthdays;
vector<TagSubStruct*> *TagSubFinals;

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

void mergeCommentsWeights(int *weights, long a[], long low, long mid,
		long high, long *b, int *bWeights) {
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

void mergesortComments(int* weights, long a[], long low, long high, long *b,
		int *bWeights) {
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

long getStrAsLong(const char *numStr, int num_sz) {
	long num = 0;
	for (int i = 0; i < num_sz; i++) {
		// dateNum = dateNum*8 + dateNum*2 = dateNum * 10
		num = (num << 3) + (num << 1);
		num += numStr[i] - '0';
	}
	return num;
}

static inline long getStrAsLong(const char *numStr) {
	long num = 0;
	for ( ;*numStr; numStr++) {
		// dateNum = dateNum*8 + dateNum*2 = dateNum * 10
		num = (num << 3) + (num << 1);
		num += *numStr - '0';
	}
	return num;
}

// takes two integers and returns a unique integer for this combination
static inline unsigned long CantorPairingFunction(long k1, long k2) {
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
	N_PERSONS = lines - 1;

	if( N_PERSONS > 20000 ){
		isLarge = 1;
	}

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
	fclose(input);

	// process the whole file in memory
	// skip the first line
	char *startLine = ((char*) memchr(buffer, '\n', LONGEST_LINE_READING)) + 1;
	char *EndOfFile = buffer + lSize;
	char *lineEnd;
	char *dateStartDivisor;
	while (startLine < EndOfFile) {
		dateStartDivisor = (char*) memchr(startLine, '|', LONGEST_LINE_READING);
		*dateStartDivisor = '\0';
		long idPerson = getStrAsLong(startLine);
		dateStartDivisor = (char*) memchr(dateStartDivisor + 1, '|', LONGEST_LINE_READING);
		dateStartDivisor = (char*) memchr(dateStartDivisor + 1, '|', LONGEST_LINE_READING);
		dateStartDivisor = (char*) memchr(dateStartDivisor + 1, '|', LONGEST_LINE_READING);

		int dateInt = getDateAsInt(dateStartDivisor + 1, 10);
		//printf("%d\n", dateInt);
		PersonBirthdays[idPerson] = dateInt;

		//for( lineEnd=dateStartDivisor+10; *lineEnd != '\n'; lineEnd++);
		lineEnd = (char*) memchr(dateStartDivisor+10, '\n', LONGEST_LINE_READING);
		startLine = lineEnd + 1;
	}
	// close the comment_hasCreator_Person
	free(buffer);

	// initialize persons
	Persons = new PersonStruct[N_PERSONS];
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
		deque<long> Q;
		Q.push_back(cPerson);
		long qIndex = 0;
		long qSize = 1;
		while (qIndex < qSize) {
			long cP = Q.front();
			Q.pop_front();
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
		printErr("readPersonKnowsPerson:: No memory while reading Person_Knows_Person!!!");
	}

	// copy the file into the buffer:
	size_t result = fread(buffer, 1, lSize, input);
	if (result != lSize) {
		printErr("readPersonKnowsPerson:: Could not read the whole file in memory!!!");
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
		long idA = getStrAsLong(startLine);
		long idB = getStrAsLong(idDivisor + 1);
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
	fclose(input);

#ifdef DEBUGGING
	char msg[100];
	long comments=0;
#endif

	CommentToPerson = new FINAL_MAP_INT_INT();

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
		long idA = getStrAsLong(startLine);
		long idB = getStrAsLong(idDivisor + 1);

		// set the person to each comment
		(*CommentToPerson)[idA] = idB;

		//printf("%d %d\n", idA, idB);

		startLine = lineEnd + 1;
#ifdef DEBUGGING
		comments++;
#endif
	}
	// close the comment_hasCreator_Person

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

	CommentsPersonToPerson = new FINAL_MAP_LONG_LONG();

	long idA,idB, personA, personB;
	// process the whole file in memory
	startLine = ((char*) memchr(buffer, '\n', LONGEST_LINE_READING)) + 1;
	EndOfFile = buffer + lSize;
	while (startLine < EndOfFile) {
		idDivisor = (char*) memchr(startLine, '|', LONGEST_LINE_READING);
		lineEnd = (char*) memchr(idDivisor, '\n', LONGEST_LINE_READING);
		*idDivisor = '\0';
		*lineEnd = '\0';
		idA = getStrAsLong(startLine);
		idB = getStrAsLong(idDivisor+1);
		//idA = getStrAsLong(startLine, idDivisor-startLine);
		//idB = getStrAsLong(idDivisor+1, lineEnd-idDivisor-1);
		startLine = lineEnd + 1;

		// get the person ids for each comment id
		personA = (*CommentToPerson)[idA];
		personB = (*CommentToPerson)[idB];

		if (personA != personB) {
			// increase the counter for the comments from A to B
			long key_a_b = CantorPairingFunction(personA, personB);
			++(*CommentsPersonToPerson)[key_a_b];
		}
		//printf("%ld %ld\n", idA, idB);

#ifdef DEBUGGING
		comments++;
#endif
	}
	fclose(input);
	free(buffer);

#ifdef DEBUGGING
	sprintf(msg, "Total replies: %ld", comments);
	printOut(msg);
#endif

}

void postProcessComments() {
	// for each person we will get each neighbor and put our edge weight in an array
	// to speed up look up time and then sort them
	long adjacentId;
	long key_a_b;
	long key_b_a;
	long weightAB;
	long weightBA;
	for (long i = 0, sz = N_PERSONS; i < sz; i++) {
		if (Persons[i].adjacents > 0) {
			long adjacents = Persons[i].adjacents;
			long *adjacentIds = Persons[i].adjacentPersonsIds;
			int *weights = (int*) malloc(sizeof(int) * adjacents);
			Persons[i].adjacentPersonWeightsSorted = weights;
			for (long cAdjacent = 0, szz = adjacents; cAdjacent < szz; cAdjacent++) {
				adjacentId = adjacentIds[cAdjacent];
				key_a_b = CantorPairingFunction(i, adjacentId);
				key_b_a = CantorPairingFunction(adjacentId, i);
				weightAB = (*CommentsPersonToPerson)[key_a_b];
				weightBA = (*CommentsPersonToPerson)[key_b_a];
				weights[cAdjacent] = ( weightAB < weightBA ) ? weightAB : weightBA;
			}
			long *temp = (long*) malloc(sizeof(long) * (adjacents));
			int *tempWeights = (int*) malloc(sizeof(int) * (adjacents));
			mergesortComments(weights, adjacentIds, 0, adjacents - 1, temp,	tempWeights);
			free(temp);
			free(tempWeights);
		}
	}
	// since we have all the data needed in arrays we can delete the hash maps
	CommentToPerson->clear();
	delete CommentToPerson;
	CommentsPersonToPerson->clear();
	delete CommentsPersonToPerson;
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
		long id = getStrAsLong(startLine);
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
		long idA = getStrAsLong(startLine);
		long idB = getStrAsLong(idDivisor + 1);

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
		long idPerson = getStrAsLong(startLine);
		long idPlace = getStrAsLong(idDivisor + 1);

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
		long idOrg = getStrAsLong(startLine);
		long idPlace = getStrAsLong(idDivisor + 1);

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
			long idPerson = getStrAsLong(startLine);
			long idOrg = getStrAsLong(idDivisor + 1);

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
		long personId = getStrAsLong(startLine);
		long tagId = getStrAsLong(idDivisor + 1);

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
		//Q2ListNode *newPerson = new Q2ListNode();
		//newPerson->birth = PersonBirthdays[personId];
		//newPerson->personId = personId;
		//(*TagSubBirthdays)[key]->people.push_back(newPerson);
		(*TagSubBirthdays)[key]->people.push_back(Q2ListNode(personId, PersonBirthdays[personId]));

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
		*nameDivisor = '\0';
		long id = getStrAsLong(startLine);
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

	Query4TagForums = new unordered_set<long>();

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
		long forumId = getStrAsLong(startLine);
		long tagId = getStrAsLong(idDivisor + 1);

		// only save the forums that are related to a tag we want
		if( Query4Tags->find(tagId) != Query4Tags->end() ){
			// insert the forum into the tag
			long tagIndex = (*TagIdToIndex)[tagId];
			Tags[tagIndex]->forums.push_back(forumId);

			Query4TagForums->insert(forumId);
		}

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
		long forumId = getStrAsLong(startLine);
		long personId = getStrAsLong(idDivisor + 1);

		if( Query4TagForums->find(forumId) != Query4TagForums->end() ){
			// insert the person directly into the forum members
			Forums[forumId].push_back(personId);
		}

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
		TagSubFinals->push_back(tagStruct);
		// sort the birthdays of each TagSubgraph
		std::stable_sort(tagStruct->people.begin(), tagStruct->people.end(),
				Q2ListNodePredicate);
	}
	// sort the final list of tags descending on the list size
	std::stable_sort(TagSubFinals->begin(), TagSubFinals->end(), Q2ListPredicate);

	// dynamic memory allocation to DELETE
	delete TagSubBirthdays;
	//TagSubBirthdays.clear();
}



///////////////////////////////////////////////////////////////////////
///////////////////// READING FILES - WORKER JOBS /////////////////////
///////////////////////////////////////////////////////////////////////

// TODO - optimization instead of adding all the jobs : KEEP ONE FOR YOURSELF

/*
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

*/










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

	char *visited = (char*) malloc(N_PERSONS);
	memset(visited, 0, N_PERSONS);
	//LPSparseArrayGeneric<long> visited;
	//vector<QueryBFS> Q;
	deque<QueryBFS> Q;

	// insert the source node into the queue
	Q.push_back(QueryBFS(p1, 0));
	unsigned long index = 0;
	unsigned long size = 1;
	while (index < size) {
		//QueryBFS current = Q[index];
		QueryBFS current = Q.front();
		Q.pop_front();
		index++;

		//printf("current: %ld %d\n", current.person, current.depth);
		// mark node as visited - BLACK
		visited[current.person] = 2;
		//visited.set(current.person,2);

		// we must add the current neighbors into the queue if
		// the comments are valid
		PersonStruct *cPerson = &Persons[current.person];
		long *adjacents = cPerson->adjacentPersonsIds;
		int *weights = cPerson->adjacentPersonWeightsSorted;
		// if there is comments limit
		if (x != -1) {
			for (long i = 0, sz = cPerson->adjacents;
					(i < sz) && (weights[i] > x); i++) {
				long cAdjacent = adjacents[i];
				if (visited[cAdjacent] == 0) {
				//if (visited.get(cAdjacent) == 0) {
					if (cAdjacent == p2) {
						answer = current.depth + 1;
						break;
					}
					//visited.set(cAdjacent,1);
					visited[cAdjacent]=1;
					Q.push_back(QueryBFS(cAdjacent, current.depth + 1));
					size++;
				}
			}
		} else {
			// no comments limit
			for (long i = 0, sz = cPerson->adjacents; i < sz; i++) {
				long cAdjacent = adjacents[i];
				// if node not visited and not added
				if (visited[cAdjacent] == 0) {
				//if (visited.get(cAdjacent) == 0) {
					if (cAdjacent == p2) {
						answer = current.depth + 1;
						break;
					}
					// mark node as added - GREY
					//visited.set(cAdjacent,1);
					visited[cAdjacent]=1;
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

long findTagLargestComponent(vector<Q2ListNode> &people, unsigned int queryBirth, long minComponentSize) {
//long findTagLargestComponent(vector<Q2ListNode*> people, unsigned int queryBirth, long minComponentSize) {
	// make the persons for this graph a set
	long indexValidPersons=0;
	//LPBitset newGraphPersons(N_PERSONS);
	LPSparseBitset newGraphPersons;
	for( unsigned long i=0,sz=people.size(); i<sz && people[i].birth >= queryBirth; i++ ){
		newGraphPersons.set(people[i].personId);
		indexValidPersons++;
	}
	/*
	for( unsigned long i=0,sz=people.size(); i<sz && people[i]->birth >= queryBirth; i++ ){
		newGraphPersons.set(people[i]->personId);
		indexValidPersons++;
	}
	*/

	// check if we have enough people to make a larger component
	if( indexValidPersons < minComponentSize ){
		return 0;
	}

	// now we have to calculate the shortest paths between them
	LPSparseArrayGeneric<long> components;
	LPSparseArrayGeneric<char> visitedBFS;
	vector<long> componentsIds;
	deque<long> Q;
	long currentCluster = -1;
	for (long i = 0, sz = indexValidPersons; i < sz; i++) {
		if( visitedBFS.get(people[i].personId) == 0 ){
			currentCluster++;
			componentsIds.push_back(currentCluster);
			Q.clear();
			Q2ListNode cPerson = people[i];
			long qIndex = 0;
			long qSize = 1;
			Q.push_back(cPerson.personId);
			while (qIndex < qSize) {
				long c = Q.front();
				Q.pop_front();
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
	for (long i = 0, sz = TagSubFinals->size(); i < sz; i++) {
		vector<Q2ListNode> &people = (*TagSubFinals)[i]->people;
		long currentTagSize = people.size();

		// do not need to process further in other tags
		if (currentTagSize < minComponentSize) {
			break;
		}
		// check the max birth date of the list in order to avoid
		// checking a list when there are no valid people
		if (people[0].birth < queryBirth) {
			continue;
		}

		// find the largest component for the current tag
		long largestTagComponent = findTagLargestComponent(people, queryBirth, minComponentSize);

		// we have to check if the current tag should be in the results
		if (currentResults < k) {
			// NOT NEEDED - initialized above
			// minComponentSize = 0;
			results.push_back(Q2ResultNode((*TagSubFinals)[i]->tagId, largestTagComponent));
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
					if ( (*itPerson).people < largestTagComponent
							|| (strcmp( TagIdToName[(*itPerson).tagId], TagIdToName[(*TagSubFinals)[i]->tagId] ) >= 0  && (*itPerson).people == largestTagComponent )) {
						// insert here
						results.insert(itPerson, Q2ResultNode((*TagSubFinals)[i]->tagId,largestTagComponent));
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
	LPBitset visited(N_PERSONS);
	//char *visited = (char*) malloc(N_PERSONS);
	//memset(visited, 0, N_PERSONS);

	deque<QueryBFS> Q;
	long qIndex = 0;
	long qSize = 1;
	Q.push_back(QueryBFS(idA, 0));
	while (qIndex < qSize) {
		QueryBFS cPerson = Q.front();
		Q.pop_front();
		qIndex++;

		// we have reached the hop limit of the query
		// so we have to exit since the person we want to reach cannot be found
		// in less than h-hops since he should have already be found
		// while pushing the neighbors below. The destination node should
		// never appear here since he will never be pushed into the Queue.
		if (cPerson.depth > h) {
			break;
		}

		long *neighbors = Persons[cPerson.person].adjacentPersonsIds;
		for (long i = 0, sz = Persons[cPerson.person].adjacents; i < sz; i++) {
			long cB = neighbors[i];
			// if person is not visited and not added yet
			//if (visited[cB] == 0) {
			if (!visited.isSet(cB)) {
				// check if this is our person
				if (idB == cB) {
					//free(visited);
					return cPerson.depth + 1;
				}
				// mark person as GREY - added
				visited.set(cB);
				//visited[cB]=1;
				Q.push_back(QueryBFS(cB, cPerson.depth + 1));
				qSize++;
			}
		}
	}
	//free(visited);
	return INT_MAX;
}

int BFS_query3(long idA, int h, unordered_set<long> &visited) {
	deque<QueryBFS> Q;
	long qIndex = 0;
	long qSize = 1;
	Q.push_back(QueryBFS(idA, 0));
	while (qIndex < qSize) {
		QueryBFS cPerson = Q.front();
		Q.pop_front();
		qIndex++;
		// we have reached the hop limit of the query
		// so we have to exit since the person we want to reach cannot be found
		// in less than h-hops since he should have already be found
		// while pushing the neighbors below. The destination node should
		// never appear here since he will never be pushed into the Queue.
		if (cPerson.depth > h) {
			break;
		}
		long *neighbors = Persons[cPerson.person].adjacentPersonsIds;
		for (long i = 0, sz = Persons[cPerson.person].adjacents; i < sz; i++) {
			long cB = neighbors[i];
			// if person is not visited and not added yet
			if( visited.count(cB) == 0 ){
				// mark person
				visited.insert(cB);
				Q.push_back(QueryBFS(cB, cPerson.depth + 1));
				qSize++;
			}
		}
	}
	//fprintf(stderr,"can reach %ld people\n", visited.size());
	return visited.size();
}

struct Query3PersonStruct{
	Query3PersonStruct(long id, int t){
		personId = id;
		numOfTags = t;
	}
	long personId;
	int numOfTags;
};
bool Query3PersonStructPredicate(const Query3PersonStruct& d1,const Query3PersonStruct& d2) {
	if (d1.numOfTags == d2.numOfTags)
		return d1.personId <= d2.personId;
	return d1.numOfTags > d2.numOfTags;
}


void query3(int k, int h, char *name, int name_sz, long qid) {
	//printf("query3 k[%d] h[%d] name[%*s] name_sz[%d]\n", k, h, name_sz, name, name_sz);

	unordered_map<int, vector<Query3PersonStruct> > ComponentsMap;

	long totalPersons=0;

	// TODO - could use unordered set for memory issues since this could be way smaller
	// like in most of the queries
	LPBitset *visitedPersons = new LPBitset(N_PERSONS);
	LPBitset *visitedPlace = new LPBitset(Places.size());
	TrieNode *place = TrieFind(PlacesToId, name, name_sz);
	long index = place->vIndex;
	deque<long> Q_places;
	Q_places.push_back(index);
	// set as added
	visitedPlace->set(index);
	long qIndex = 0;
	long qSize = 1;
	while (qIndex < qSize) {
		long cPlace = Q_places.front();
		Q_places.pop_front();
		qIndex++;
		// set visited
		PlaceNodeStruct *cPlaceStruct = Places[cPlace];
		std::vector<long>::iterator cPerson = cPlaceStruct->personsThis.begin();
		std::vector<long>::iterator end = cPlaceStruct->personsThis.end();
		for (; cPerson != end; cPerson++) {
			if (visitedPersons->isSet(*cPerson))
				continue;
			visitedPersons->set(*cPerson);
			ComponentsMap[Persons[*cPerson].subgraphNumber].push_back(Query3PersonStruct(
					*cPerson, PersonToTags[*cPerson].tags.size()));

			totalPersons++;
		}

		for (std::vector<long>::iterator it =
				cPlaceStruct->placesPartOfIndex.begin();
				it != cPlaceStruct->placesPartOfIndex.end(); ++it) {
			// if not visited
			if (visitedPlace->isSet(*it) == 0) {
				// set as added
				visitedPlace->set(*it);
				Q_places.push_back(*it);
				qSize++;
			}
		}
	}
	//delete[] visitedPlace;
	delete visitedPlace;
	delete visitedPersons;

	fprintf(stderr, "3[%d-%lu]",k, totalPersons);
	/*
	if( isLarge ){
		fprintf(stderr, "3[%d-%lu]",k, totalPersons);
		return;
	}
	*/

	//printf("found for place [%*s] persons[%ld] index[%ld]\n", name_sz, name, persons.size(), index);


	// the global queue that will hold the Top-K pairs
	priority_queue<Query3PQ, vector<Query3PQ>, Query3PQ_ComparatorMin> GlobalPQ;
	GlobalPQ.push(Query3PQ(INT_MAX,INT_MAX,0)); // insert the sentinel with common tags

	// for each cluster calculate the common tags and check if we have a new Top-K pair
	unordered_map<int, vector<Query3PersonStruct> >::iterator clBegin = ComponentsMap.begin();
	unordered_map<int, vector<Query3PersonStruct> >::iterator clEnd = ComponentsMap.end();
	unordered_set<long> visited;
	for( ; clBegin != clEnd; clBegin++ ){
		vector<Query3PersonStruct> *currentClusterPersons = &((*clBegin).second);
		// we cannot find pairs in 1-person clusters
		if( currentClusterPersons->size() < 2 )
			continue;
		std::stable_sort(currentClusterPersons->begin(), currentClusterPersons->end(), Query3PersonStructPredicate);
		int minimumCommonTags = GlobalPQ.top().commonTags;
		// since the maximum tags of this cluster are less than the global minimum
		// there is no chance to find a valid pair
		if( (GlobalPQ.size()-1) >= k ){
			if( currentClusterPersons->at(0).numOfTags < minimumCommonTags )
				continue;
			if( currentClusterPersons->at(1).numOfTags < minimumCommonTags )
				continue;
		}
		// for each person in the cluster
		for( int i=0, sz=currentClusterPersons->size()-1; i<sz; i++ ){
			// we cannot find suitable common tags by this person since his tags are less
			// than the current minimum
			Query3PersonStruct *currentPerson = &(currentClusterPersons->at(i));
			if( GlobalPQ.size() >= (unsigned int)k && currentPerson->numOfTags < minimumCommonTags )
				//continue;// TODO
				break;
			visited.clear();
			// we have to do a BFS from this person until max-k hops to find our reachability
			// in order to avoid calculating common tags with people that are further than necessary
			// TODO - check if it is faster to be done for a pair each time after having a valid common tag number
			BFS_query3(currentPerson->personId, h, visited);
			for( int j=i+1, szz=currentClusterPersons->size(); j<szz; j++ ){
				// TODO -  ADD A CHECK FOR THE TAGS NUMBER AND EXIT QUICKLY SINCE THEY ARE SORTED
				Query3PersonStruct *secondPerson = &currentClusterPersons->at(j);
				// skip him if we know for sure that we are more than h-hops away
				//if( visited.count(secondPerson->personId) == 0 )
					//continue;
				// we now have to calculate the common tags between these two people
				int cTags = 0;
				vector<long> *tagsA = &PersonToTags[currentPerson->personId].tags;
				vector<long> *tagsB = &PersonToTags[secondPerson->personId].tags;
				std::vector<long>::const_iterator iA = tagsA->begin();
				std::vector<long>::const_iterator endA = tagsA->end();
				std::vector<long>::const_iterator iB = tagsB->begin();
				std::vector<long>::const_iterator endB = tagsB->end();
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
				}// end of common tags calculation

				// just insert the new pair in the answers - we have to take into account the sentinel element
				if( GlobalPQ.size() < (unsigned int)k ){
					if (currentPerson->personId <= secondPerson->personId) {
						GlobalPQ.push(Query3PQ(currentPerson->personId, secondPerson->personId, cTags));
					} else {
						GlobalPQ.push(Query3PQ(secondPerson->personId, currentPerson->personId, cTags));
					}
					minimumCommonTags = GlobalPQ.top().commonTags;
				}else{
					// we have to check if the new pair has more common tags than the answers so far
					long leftId, rightId;
					if (currentPerson->personId <= secondPerson->personId) {
						leftId = currentPerson->personId;
						rightId = secondPerson->personId;
					}else{
						rightId = currentPerson->personId;
						leftId = secondPerson->personId;
					}
					if( Query3PQ_ComparatorStatic( GlobalPQ.top(), leftId, rightId, cTags ) ){
						GlobalPQ.pop();
						GlobalPQ.push(Query3PQ(leftId, rightId, cTags));
						minimumCommonTags = GlobalPQ.top().commonTags;
					}
				}
			}// end of checking pairs for current person
		}// end of cluster's people
	}// end of processing the clusters

	// now we have to pop the K most common tag pairs
	// but we also have to check that the distance between them
	// is below the H-hops needed by the query.

	vector<Query3PQ> answers;
	std::stringstream ss;
	if( GlobalPQ.top().idA == GlobalPQ.top().idB )
			GlobalPQ.pop();
	for (; k > 0; k--) {
		if( GlobalPQ.empty() )
			break;
		answers.push_back(GlobalPQ.top());
		GlobalPQ.pop();
	}
	for( int i=answers.size()-1; i>=0; i-- )
		ss << answers[i].idA << "|" << answers[i].idB << " ";
	Answers[qid] = ss.str();

	/*
	// now we have to pop the K most common tag pairs
	// but we also have to check that the distance between them
	// is below the H-hops needed by the query.
	if( GlobalPQ.top().idA == GlobalPQ.top().idB )
				GlobalPQ.pop();
	std::stringstream ss;
	for (; k > 0; k--) {
		if (GlobalPQ.empty())
			break;
		long idA, idB;
		//long cTags = -1;
		while (!GlobalPQ.empty()) {
			const Query3PQ cPair = GlobalPQ.top();
			idA = cPair.idA;
			idB = cPair.idB;

			//int cTags = cPair.commonTags;
			GlobalPQ.pop();
			int distance = BFS_query3(idA, idB, h);
			if (distance <= h) {
				// we have an answer so exit the while
				ss << idA << "|" << idB << " ";
				break;
			}
		}
		//ss << idA << "|" << idB << "[" << cTags << "] ";
	}
	Answers[qid] = ss.str();
	*/
}

//////////////////////////////////////////////////////////////////////
// QUERY 4
//////////////////////////////////////////////////////////////////////

struct Q4InnerJob{
	int start;
	int end;
	vector<Query4PersonStruct> *persons;
	MAP_INT_VecL *newGraph;
};

void *Query4InnerWorker(void *args){
	Q4InnerJob *qArgs = (Q4InnerJob*)args;

	vector<Query4PersonStruct> *persons = qArgs->persons;
	MAP_INT_VecL *newGraph = qArgs->newGraph;

	// now we have to calculate the shortest paths between them
	int n_1 = persons->size() - 1;
	//MAP_LONG_CHAR visitedBFS;
	LPSparseArrayGeneric<char> visitedBFS;
	deque<QueryBFS> Q;
	for (int i = qArgs->start, sz = qArgs->end; i < sz; i++) {
		visitedBFS.clear();
		Q.clear();
		// create a local copy of the struct
		Query4PersonStruct cPerson = (*persons)[i];
		long qIndex = 0;
		long qSize = 1;
		Q.push_back(QueryBFS(cPerson.person, 0));
		while (qIndex < qSize) {
			//QueryBFS &c = Q[qIndex];
			QueryBFS c = Q.front();
			Q.pop_front();
			qIndex++;
			//visitedBFS[c.person] = 2;
			visitedBFS.set(c.person, 2);
			// update info for the current person centrality
			cPerson.s_p += c.depth;

			// insert each unvisited neighbor of the current node
			vector<long> &edges = (*newGraph)[c.person];
			//long*edges = Persons[c.person].adjacentPersonsIds;
			for (int e = 0, szz = edges.size(); e < szz; e++) {
				//for (int e = 0, szz = Persons[c.person].adjacents; e < szz; e++) {
				//long eId = (*edges)[e];
				long eId = edges[e];
				//if (visitedBFS[eId] == 0) {
				if (visitedBFS.get(eId) == 0) {
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
			cPerson.centrality = ((double) (cPerson.r_p * cPerson.r_p))/ (n_1 * cPerson.s_p);
		}
		// assign the calculated person to the vector
		(*persons)[i] = cPerson;
	}

	return 0;
}

void query4(int k, char *tag, int tag_sz, long qid, int tid) {
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

	if(isLarge){
		fprintf(stderr, "4[%lu]", persons.size());
		return;
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

	// calculate the closeness centrality for all people in the person vector

	const static int Q4_threads = Q4_JOB_WORKERS;

	int totalJobs = persons.size();
	int perThreadJobs = totalJobs / Q4_threads;
	int untilThreadJobsPlus = totalJobs % Q4_threads;
	int lastEnd = 0;
	pthread_t *worker_threads = (pthread_t*) malloc(sizeof(pthread_t) * Q4_threads);
	cpu_set_t mask;
	int i=0;
	for ( ; i < Q4_threads-1; i++) {
		Q4InnerJob *qws = (Q4InnerJob*) malloc(sizeof(Q4InnerJob));
		qws->start = lastEnd;
		if (i < untilThreadJobsPlus) {
			lastEnd += perThreadJobs + 1;
		} else {
			lastEnd += perThreadJobs;
		}
		qws->end = lastEnd;
		qws->newGraph = &newGraph;
		qws->persons = &persons;

		pthread_create(&worker_threads[i], NULL,reinterpret_cast<void* (*)(void*)>(Query4InnerWorker), qws );
		//fprintf( stderr, "[%ld] thread[%d] added\n", worker_threads[i], i );
	}

	// execute some calculations yourself
	Q4InnerJob *qws = (Q4InnerJob*) malloc(sizeof(Q4InnerJob));
	qws->start = lastEnd;
	if (i < untilThreadJobsPlus) {
		lastEnd += perThreadJobs + 1;
	} else {
		lastEnd += perThreadJobs;
	}
	qws->end = lastEnd;
	qws->newGraph = &newGraph;
	qws->persons = &persons;
	Query4InnerWorker(qws);

	// wait for them to finish
	for (int i = 0; i < Q4_threads-1; i++) {
		pthread_join(worker_threads[i], NULL);
	}

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

struct QWorker{
	int start;
	int end;
};

vector<Query1WorkerStruct*> Query1Structs;

void* Query1WorkerFunction(void *args) {
	QWorker *qws = (QWorker*)args;
	Query1WorkerStruct *currentJob;

	for( int i=qws->start, end=qws->end; i<end; i++ ){
		currentJob = Query1Structs[i];
		query1(currentJob->p1, currentJob->p2, currentJob->x, currentJob->qid);
		free(currentJob);
		// the following can be omitted for speedups
		//Query1Structs[i] = 0;
	}

	free(qws);
	pthread_exit(NULL);
	// end of job
	return 0;
}

void executeQuery1Jobs(int q1threads){
	int totalJobs = Query1Structs.size();
	int perThreadJobs = totalJobs / q1threads;
	int untilThreadJobsPlus = totalJobs % q1threads;
	int lastEnd = 0;
	pthread_t *worker_threads = (pthread_t*)malloc(sizeof(pthread_t)*q1threads);
	for (int i = 0; i < q1threads; i++) {
		QWorker *qws = (QWorker*)malloc(sizeof(QWorker));
		qws->start = lastEnd;
		if( i < untilThreadJobsPlus ){
			lastEnd += perThreadJobs + 1;
		}else{
			lastEnd += perThreadJobs;
		}
		qws->end = lastEnd;
		pthread_create(&worker_threads[i], NULL,reinterpret_cast<void* (*)(void*)>(Query1WorkerFunction), qws );
		//fprintf( stderr, "[%ld] thread[%d] added\n", worker_threads[i], i );
	}

	// DO NOT NEED TO wait for them to finish for now since we are reading files at the same time
	for (int i = 0; i < q1threads; i++) {
		pthread_join(worker_threads[i], NULL);
	}
	// free all the memory being held for the comments
	for( long i=0; i<N_PERSONS; i++ ){
		free(Persons[i].adjacentPersonWeightsSorted);
		Persons[i].adjacentPersonWeightsSorted = 0;
	}
}

void *readCommentsAsyncWorker(void *args){
	long time_ = getTime();
	readComments(inputDir);
	fprintf(stderr, "finished reading comments [%.8f]\n", (getTime()-time_)/1000000.0);
	time_ = getTime();
	postProcessComments();
	fprintf(stderr, "finished post processing comments [%.8f]\n", (getTime()-time_)/1000000.0);
	fprintf(stderr, "finished processing comments\n");
	return 0;
}

pthread_t* readCommentsAsync(){
	pthread_t* cThread = (pthread_t*)malloc(sizeof(pthread_t));
	pthread_create(cThread, NULL,reinterpret_cast<void* (*)(void*)>(readCommentsAsyncWorker), NULL );
	cpu_set_t mask;
	CPU_ZERO(&mask);
	CPU_SET( 1 , &mask);
	return cThread;
}

struct Query2WorkerStruct {
	int k;
	char *date;
	int date_sz;
	long qid;
};

vector<Query2WorkerStruct*> Query2Structs;

void* _destroyQ2Index(void *args){
	Query2Structs.clear();
	for( long i=0,sz=TagSubFinals->size(); i<sz; i++ ){
		delete (*TagSubFinals)[i];
	}
	delete TagSubFinals;
	return 0;
}

void* Query2WorkerFunction(void *args) {
	QWorker *qws = (QWorker*)args;
	Query2WorkerStruct *currentJob;

	for( int i=qws->start, end=qws->end; i<end; i++ ){
		currentJob = Query2Structs[i];
		query2(currentJob->k, currentJob->date, currentJob->date_sz, currentJob->qid);
		free(currentJob);
		// the following can be omitted for speedups
		Query2Structs[i] = 0;
	}

	free(qws);
	// end of job
	return 0;
}

pthread_t *worker2_threads;
int global_Q2threads;
void* Query2MasterWorkerFunction(void *args) {
	// execute the jobs regularly
	Query2WorkerFunction(args);
	// end of job

	// wait for them to finish
	for (int i = 1; i < global_Q2threads; i++) {
		pthread_join(worker2_threads[i], NULL);
	}
	_destroyQ2Index(NULL);

	pthread_exit(NULL);
	return 0;
}

void executeQuery2Jobs(int q2threads){
	global_Q2threads = q2threads;
	int totalJobs = Query2Structs.size();
	int perThreadJobs = totalJobs / q2threads;
	int untilThreadJobsPlus = totalJobs % q2threads;
	int lastEnd = 0;
	worker2_threads = (pthread_t*)malloc(sizeof(pthread_t)*q2threads);
	for (int i = 0; i < q2threads; i++) {
		QWorker *qws = (QWorker*)malloc(sizeof(QWorker));
		qws->start = lastEnd;
		if( i < untilThreadJobsPlus ){
			lastEnd += perThreadJobs + 1;
		}else{
			lastEnd += perThreadJobs;
		}
		qws->end = lastEnd;
		if( i==0 ){
			pthread_create(&worker2_threads[i], NULL,reinterpret_cast<void* (*)(void*)>(Query2MasterWorkerFunction), qws );
		}else{
			pthread_create(&worker2_threads[i], NULL,reinterpret_cast<void* (*)(void*)>(Query2WorkerFunction), qws );
		}
	}
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
	query4(qArgs->k, qArgs->tag, qArgs->tag_sz, qArgs->qid, tid);

	free(qArgs->tag);
	free(qArgs);
	// end of job
	return 0;
}

///////////////////////////////////////////////////////////////////////

///////////////////////////////////////////////////////////////////////
// MAIN PROGRAM
///////////////////////////////////////////////////////////////////////

void _initializations() {
	PlaceIdToIndex = new FINAL_MAP_INT_INT();
	OrgToPlace = new FINAL_MAP_INT_INT();

	// Q2
	TagSubFinals = new vector<TagSubStruct*>();

	PlacesToId = TrieNode_Constructor();
	Places.reserve(2048);

	TagToIndex = TrieNode_Constructor();
	Tags.reserve(2048);
	TagIdToIndex = new FINAL_MAP_INT_INT();

}

void _destructor() {
	//_destoryQ2Index();
	delete[] Persons;
	delete[] PersonToTags;
	TrieNode_Destructor(PlacesToId);
	TrieNode_Destructor(TagToIndex);
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
		int queryType = atol(startLine + 5);

		// handle the new query
		switch (queryType) {
		case 1: {
			char *second = ((char*) memchr(startLine + 7, ',', LONGEST_LINE_READING)) + 1;
			*(second - 1) = '\0';
			char *third = ((char*) memchr(second, ',', LONGEST_LINE_READING)) + 1;
			*(lineEnd - 1) = '\0';
			//query1(getStrAsLong(startLine+7), getStrAsLong(second), getStrAsLong(third), qid);

			Query1WorkerStruct *qwstruct = (Query1WorkerStruct*) malloc(sizeof(Query1WorkerStruct));
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
			//query2(getStrAsLong(startLine + 7), date, lineEnd - 1 - date, qid);

			Query2WorkerStruct *qwstruct = (Query2WorkerStruct*) malloc(sizeof(Query2WorkerStruct));
			qwstruct->k = atol(startLine + 7);
			qwstruct->date_sz = lineEnd-1-date;
			qwstruct->date = strndup(date, qwstruct->date_sz);
			qwstruct->qid = qid;
			//lp_threadpool_addjob_nolock(threadpool,reinterpret_cast<void* (*)(int,void*)>(Query2WorkerFunction), (void*)qwstruct );
			Query2Structs.push_back(qwstruct);

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
			//query3(getStrAsLong(startLine + 7), getStrAsLong(second), name, name_sz, qid);

			char *placeName = (char*) malloc(name_sz + 1);
			strncpy(placeName, name, name_sz + 1);
			Query3WorkerStruct *qwstruct = (Query3WorkerStruct*) malloc(sizeof(Query3WorkerStruct));
			qwstruct->k = atol(startLine + 7);
			qwstruct->h = atol(second);
			qwstruct->name = placeName;
			qwstruct->name_sz = name_sz;
			qwstruct->qid = qid;
			lp_threadpool_addjob_nolock(threadpool3,reinterpret_cast<void* (*)(int,void*)>(Query3WorkerFunction), qwstruct );

			break;
		}
		case 4: {
			char *second = ((char*) memchr(startLine + 7, ',', LONGEST_LINE_READING)) + 1;
			*(second - 1) = '\0';
			*(lineEnd - 1) = '\0';
			char *name = second + 1; // to skip one space
			int tag_sz = lineEnd - 1 - name;
			//query4(getStrAsLong(startLine + 7), name, tag_sz, qid);

			char *tagName = (char*) malloc(tag_sz + 1);
			strncpy(tagName, name, tag_sz + 1);
			Query4WorkerStruct *qwstruct = (Query4WorkerStruct*) malloc(sizeof(Query4WorkerStruct));
			qwstruct->k = atol(startLine + 7);
			qwstruct->tag = tagName;
			qwstruct->tag_sz = tag_sz;
			qwstruct->qid = qid;
			lp_threadpool_addjob_nolock(threadpool4,reinterpret_cast<void* (*)(int,void*)>(Query4WorkerFunction), qwstruct );

			// TODO - add the asked Tag into the set
			Query4Tags->insert(TrieFind(TagToIndex, tagName, tag_sz)->realId);

			break;
		}
		default: {
			*lineEnd = '\0';
			//printOut(startLine);
		}
		}
		startLine = lineEnd + 1;
		qid++;
	}
	free(buffer);
}

int main(int argc, char** argv) {

	inputDir = argv[1];
	queryFile = argv[2];

	// make the master thread to run only on the 1st core
	/*
	cpu_set_t mask;
	CPU_ZERO(&mask);
	CPU_SET( 0 , &mask);
	*/

	// MAKE GLOBAL INITIALIZATIONS
	char msg[100];
	_initializations();

	long long time_global_start = getTime();

#ifdef DEBUGGING
	long time_queries_end = getTime();
	sprintf(msg, "queries file time: %ld", time_queries_end - time_global_start);
	printOut(msg);
#endif

	/////////////////////////////////
	readPersons(inputDir);
	readPersonKnowsPerson(inputDir);

	///////////////////////////////////////////////////////////////////
	// PROCESS THE COMMENTS OF EACH PERSON A
	// - SORT THE EDGES BASED ON THE COMMENTS from A -> B
	///////////////////////////////////////////////////////////////////
	//pthread_t *commentsThread = readCommentsAsync();
	readCommentsAsyncWorker(NULL);


	// Q4 - we read this first in order to read the queries file now
	readTags(inputDir);

	// HERE WE READ THE QUERIES IN ORDER TO DETERMINE WHICH TAGS ARE REQUIRED BY THE QUERIES
	threadpool3 = lp_threadpool_init( Q3_THREADPOOOL_THREADS, NUM_CORES);
	threadpool4 = lp_threadpool_init( Q4_THREADPOOOL_THREADS, NUM_CORES);
	Query4Tags = new unordered_set<long>();
	readQueries(queryFile);
	///////////////////////////////////

	// now we can start executing QUERY 1 - we use WORKER_THREADS
	//pthread_join(*commentsThread, NULL);
	executeQuery1Jobs(Q1_WORKER_THREADS);
	fprintf(stderr,"query 1 finished %.6fs\n", (getTime()-time_global_start)/1000000.0);


#ifdef DEBUGGING
	long time_persons_end = getTime();
	sprintf(msg, "persons graph time: %ld", time_persons_end - time_global_start);
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

	// Q3 - last file needed
	readPersonHasInterestTag(inputDir);

	// Q2 - requirement
	postProcessTagBirthdays();

	// execute the queries 2 and destroy the index
	executeQuery2Jobs(Q2_WORKER_THREADS);

	// required by 4
	readForumHasTag(inputDir);
	readForumHasMember(inputDir);
	delete Query4Tags;
	delete Query4TagForums;


#ifdef DEBUGGING
	long time_tags_end = getTime();
	sprintf(msg, "tags process time: %ld", time_tags_end - time_global_start);
	printOut(msg);
#endif

	fprintf(stderr, "finished processing all other files [%.6f]\n", (getTime()-time_global_start)/1000000.0);

	// start workers for Q3
	lp_threadpool_startjobs(threadpool3);
	synchronize_complete(threadpool3);
	//fprintf(stderr,"query 3 finished %.6fs\n", (getTime()-time_q3_start)/1000000.0);
	fprintf(stderr,"query 3 finished %.6fs\n", (getTime()-time_global_start)/1000000.0);


	// start workers for Q4
	lp_threadpool_startjobs(threadpool4);
	synchronize_complete(threadpool4);
	//fprintf(stderr,"query 4 finished %.6fs\n", (getTime()-time_q4_start)/1000000.0);
	fprintf(stderr,"query 4 finished %.6fs\n", (getTime()-time_global_start)/1000000.0);


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
	//_destructor();
}

////////////////////////////////////////////////////////////////////////
// TRIE IMPLEMENTATION
////////////////////////////////////////////////////////////////////////
TrieNode* TrieNode_Constructor() {
	TrieNode* n = (TrieNode*) malloc(sizeof(TrieNode));
	if (!n)
		printErr("error allocating TrieNode");
	n->realId = -1;
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
	if (-1 != node->realId) {
		return node;
	}
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
	if (found && root->realId != -1) {
		// WE HAVE A MATCH SO return the node
		return root;
	}
	return 0;
}


////////////////////////////////////////////////////////////////////////
// COMMENT TRIE IMPLEMENTATION
////////////////////////////////////////////////////////////////////////
CommentTrieNode* CommentTrieNode_Constructor() {
	CommentTrieNode* n = (CommentTrieNode*) malloc(sizeof(CommentTrieNode));
	if (!n)
		printErr("error allocating CommentTrieNode");
	n->personId = -1;
	memset(n->children, 0, 10 * sizeof(CommentTrieNode*));
	return n;
}
void CommentTrieNode_Destructor(CommentTrieNode* node) {
	for (int i = 0; i < 10; i++) {
		if (node->children[i] != 0) {
			CommentTrieNode_Destructor(node->children[i]);
		}
	}
	free(node);
}
CommentTrieNode* CommentTrieInsert(CommentTrieNode* node, const char* commentId, char id_sz, long personId) {
	int ptr = id_sz-1;
	int pos;
	while (ptr >= 0 ) {
		pos = commentId[ptr]-'0';
		if (node->children[pos] == 0) {
			node->children[pos] = CommentTrieNode_Constructor();
		}
		node = node->children[pos];
		ptr--;
	}
	// if already exists we do not overwrite but just return the existing one
	if( node->personId != -1 ){
		return node;
	}
	node->personId = personId;
	return node;
}
CommentTrieNode* CommentTrieFind(CommentTrieNode* root, const char* commentId, char id_sz) {
	int p, i, found = 1;
	for (p = id_sz-1; p >=0; p--) {
		i = commentId[p]-'0';
		if (root->children[i] != 0) {
			root = root->children[i];
		} else {
			found = 0;
			break;
		}
	}
	if (found && root->personId != -1) {
		// WE HAVE A MATCH SO return the node
		return root;
	}
	return 0;
}


