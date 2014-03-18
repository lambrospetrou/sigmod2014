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
#include <vector>
#include <queue>
#include <map>
#include <algorithm>
#include <iterator>
#include <sstream>

#include <tr1/unordered_map>

#include <bitset>

using namespace std;
using std::tr1::unordered_map;
using std::tr1::hash;

#define JOIN(x, y) JOIN_AGAIN(x, y)
#define JOIN_AGAIN(x, y) x ## y

#define DEBUGGING 1
#define FILE_VBUF_SIZE 8192
#define FILE_BUFFER_SIZE 1<<15

#define VALID_PLACE_CHARS 256

///////////////////////////////////////////////////////////////////////////////
// structs
///////////////////////////////////////////////////////////////////////////////

typedef vector<int> LIST_INT;
//typedef map<int, int> MAP_INT_INT;
typedef std::tr1::unordered_map<int, int, hash<int> > MAP_INT_INT;

struct PersonStruct {
	PersonStruct() {
		subgraphNumber = -1;
		//adjacentPersons.reserve(32);
		adjacents = 0;
		adjacentPersonsIds = NULL;
		adjacentPersonWeightsSorted = NULL;
	}
	//LIST_INT adjacentPersons;
	long *adjacentPersonsIds;
	long adjacents;

	long *adjacentPersonWeightsSorted;

	int subgraphNumber;
};

struct PersonCommentsStruct {
	MAP_INT_INT commentsToPerson;
	MAP_INT_INT adjacentPersonWeights;
};

struct TrieNode{
	char valid;
	long placeId;
	long placeIndex;
	//vector<long> sameNameIds;
	TrieNode* children[VALID_PLACE_CHARS];
};

struct PlaceNodeStruct{
	long id;
	long index;
	vector<long> personsThis;
	vector<long> placesPartOfIndex;
};

struct TagNode{
	long id;
	TrieNode *tagNode;
};

struct PersonTags{
	vector<long> tags;
};

/////////////////////////////
// QUERY SPECIFIC
/////////////////////////////
struct QueryBFS{
	QueryBFS(long id, long d){
		person = id;
		depth = d;
	}
	long person;
	int depth;
};

struct Query3PQ{
	Query3PQ(long a, long b, int ct){
		idA = a;
		idB = b;
		commonTags = ct;
	}
	long idA;
	long idB;
	int commonTags;
};
class Query3PQ_Comparator{
public:
    bool operator() (Query3PQ left, Query3PQ right){
        if( left.commonTags > right.commonTags )
        	return false;
        if( left.commonTags < right.commonTags )
        	return true;
        if( left.idA < right.idA )
        	return false;
        if( left.idA > right.idA )
        	return true;
        if( left.idB < right.idB )
        	return false;
        return false;
    }
};


///////////////////////////////////////////////////////////////////////////////
// FUNCTION PROTOTYPES
///////////////////////////////////////////////////////////////////////////////

TrieNode* TrieNode_Constructor();
void TrieNode_Destructor( TrieNode* node );
TrieNode* TrieInsert( TrieNode* node, const char* name, char name_sz, long id, long index);
TrieNode* TrieFind( TrieNode* root, const char* name, char name_sz );




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
char *CSV_PLACE = "/place_utf8.csv";
char *CSV_PLACE_PART_OF_PLACE = "/place_isPartOf_place.csv";
char *CSV_PERSON_LOCATED_AT_PLACE = "/person_isLocatedIn_place.csv";
char *CSV_ORGANIZATION_LOCATED_AT_PLACE = "/organisation_isLocatedIn_place.csv";

char *CSV_PERSON_STUDYAT_ORG = "/person_studyAt_organisation.csv";
char *CSV_WORKAT_ORG = "/person_workAt_organisation.csv";

char *CSV_TAG = "/tag.csv";
char *CSV_PERSON_HASINTEREST_TAG = "/person_hasInterest_tag.csv";

long N_PERSONS = 0;
long N_TAGS = 0;
long N_SUBGRAPHS = 0;

PersonStruct *Persons;
TrieNode *PlacesToId;
vector<PlaceNodeStruct*> Places;
PersonTags *PersonToTags;

vector<int> Answers1;
vector<string> Answers3;

// the structures below are only used as intermediate steps while
// reading the comments files. DO NOT USE THEM ANYWHERE
PersonCommentsStruct *PersonsComments;
MAP_INT_INT *CommentToPerson;

MAP_INT_INT *PlaceIdToIndex;
MAP_INT_INT *OrgToPlace;

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

void mergeCommentsWeights(long *weights, long a[], long low, long mid, long high, long *b, long *bWeights)
{
    long i = low, j = mid + 1, k = low;

    while (i <= mid && j <= high) {
    	//if (weights[i] <= weights[j]){
    	if (weights[i] >= weights[j]){
            b[k] = a[i];
            bWeights[k] = weights[i];
            k++; i++;
        }else{
        	b[k] = a[j];
        	bWeights[k] = weights[j];
        	k++; j++;
        }
    }
    while (i <= mid){
    	b[k] = a[i];
    	bWeights[k] = weights[i];
    	k++; i++;
    }

    while (j <= high){
    	b[k] = a[j];
    	bWeights[k] = weights[j];
    	k++; j++;
    }

    k--;
    while (k >= low) {
        a[k] = b[k];
        weights[k] = bWeights[k];
        k--;
    }
}

void mergesortComments(long* weights, long a[], long low, long high, long *b, long *bWeights)
{
    if (low < high) {
        long m = ((high - low)>>1)+low;
        mergesortComments(weights, a, low, m, b , bWeights);
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

long countFileLines(char *file){
	//static const auto BUFFER_SIZE = 16 * 1024;
	int fd = open(file, O_RDONLY);
	if (fd == -1)
		printErr("Error while opening file for line counting");

	/* Advise the kernel of our access pattern.  */
	posix_fadvise(fd, 0, 0, 1);  // FDADVICE_SEQUENTIAL

	char buf[(FILE_BUFFER_SIZE) + 1];
	long lines = 0;
	long bytes_read;

	while ( (bytes_read = read(fd, buf, FILE_BUFFER_SIZE)) > 0) {
		if (bytes_read == -1)
			printErr("countFileLines()::Could not read from file!");
		if (!bytes_read)
			break;
		for (char *p = buf;
				(p = (char*) memchr(p, '\n', (buf + bytes_read) - p));
				++p)
			++lines;
	}

	return lines;
}

long getFileSize(FILE *file){
	fseek(file, 0, SEEK_END);
	long lSize = ftell(file);
	rewind(file);
	return lSize;
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

	// initialize persons
	//Persons = malloc(sizeof(PersonStruct)*N_PERSONS);
	Persons = new PersonStruct[N_PERSONS];
	PersonsComments = new PersonCommentsStruct[N_PERSONS];
	PersonToTags = new PersonTags[N_PERSONS];
}

long calculateAndAssignSubgraphs(){
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

void readPersonKnowsPerson(char *inputDir){
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

	long edges=0;

	// the whole file is now loaded in the memory buffer.
	vector<long> ids;
	ids.reserve(256);
	long prevId = -1;
	// skip the first line
	char *startLine = ((char*) memchr(buffer, '\n', 100)) + 1;
	char *EndOfFile = buffer + lSize;
	while (startLine < EndOfFile) {
		char *lineEnd = (char*) memchr(startLine, '\n', 100);
		char *idDivisor = (char*) memchr(startLine, '|', lineEnd - startLine);
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
				person->adjacentPersonsIds = (long*)malloc(sizeof(long)*ids.size());
				for( long i=0,sz=ids.size(); i<sz; i++ ){
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
	if( !ids.empty() ){
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



char* getFileBytes(FILE *file, long *lSize){
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

void postProcessComments(){
	// for each person we will get each neighbor and put our edge weight in an array
	// to speed up look up time and then sort them
	for( long i=0,sz=N_PERSONS; i<sz; i++ ){
		if( Persons[i].adjacents > 0 ){
			long adjacents = Persons[i].adjacents;
			long *adjacentIds = Persons[i].adjacentPersonsIds;
			MAP_INT_INT *weightsMap = &(PersonsComments[i].adjacentPersonWeights);
			Persons[i].adjacentPersonWeightsSorted = (long*)malloc(sizeof(long)*adjacents);
			long *weights = Persons[i].adjacentPersonWeightsSorted;
			for( long cAdjacent=0,szz=adjacents; cAdjacent<szz; cAdjacent++){
				weights[cAdjacent] = (*(weightsMap))[adjacentIds[cAdjacent]];
			}
			// now we need to sort them
			/*
			printf("\n\nUnsorted: \n");
			for( long cAdjacent=0,szz=adjacents; cAdjacent<szz; cAdjacent++){
				printf("[%ld,%ld] ",Persons[i].adjacentPersonsIds[cAdjacent], weights[cAdjacent]);
			}
			 */
			long *temp = (long*)malloc(sizeof(long)*(adjacents));
		    long *tempWeights = (long*)malloc(sizeof(long)*(adjacents));
			mergesortComments(weights, adjacentIds, 0, adjacents-1, temp, tempWeights);
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
	char *startLine = ((char*) memchr(buffer, '\n', 100)) + 1;
	char *EndOfFile = buffer + lSize;
	char *lineEnd;
	char *idDivisor;
	while (startLine < EndOfFile) {
		lineEnd = (char*) memchr(startLine, '\n', 100);
		idDivisor = (char*) memchr(startLine, '|', lineEnd - startLine);
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
		int len = EndOfFile - startLine;
		lineEnd = (char*) memchr(startLine, '\n', len);
		idDivisor = (char*) memchr(startLine, '|', len);
		*idDivisor = '\0';
		if( lineEnd != NULL ){
			*lineEnd = '\0';
		}else{
			lineEnd = EndOfFile;
		}
		long idA = atol(startLine);
		long idB = atol(idDivisor + 1);

		// we have to hold the number of comments between each person
		long personA = (*CommentToPerson)[idA];
		long personB = (*CommentToPerson)[idB];

		if( personA != personB ){
			// increase the counter for the comments from A to B
			int a_b = PersonsComments[personA].commentsToPerson[personB] + 1;
			PersonsComments[personA].commentsToPerson[personB] = a_b;

			///////////////////////////////////////////////////////////////////
			// - Leave only the min(comments A-to-B, comments B-to-A) at each edge
			///////////////////////////////////////////////////////////////////
			int b_a = PersonsComments[personB].commentsToPerson[personA];
			if( a_b <= b_a ){
				PersonsComments[personA].adjacentPersonWeights[personB] = a_b;
				PersonsComments[personB].adjacentPersonWeights[personA] = a_b;
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

	///////////////////////////////////////////////////////////////////
	// PROCESS THE COMMENTS OF EACH PERSON A
	// - SORT THE EDGES BASED ON THE COMMENTS from A -> B
	///////////////////////////////////////////////////////////////////
	postProcessComments();

}



void readPlaces(char *inputDir){
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

	long places=0;
	// process the whole file in memory
	// skip the first line
	char *startLine = ((char*) memchr(buffer, '\n', 100)) + 1;
	char *EndOfFile = buffer + lSize;
	char *lineEnd;
	char *idDivisor;
	char *nameDivisor;
	while (startLine < EndOfFile) {
		int len = EndOfFile - startLine;
		lineEnd = (char*) memchr(startLine, '\n', len);
		idDivisor = (char*) memchr(startLine, '|', len);
		nameDivisor = (char*) memchr(idDivisor+1, '|', len);
		*idDivisor = '\0';
		*lineEnd = '\0';
		*nameDivisor = '\0';
		long id = atol(startLine);
		char *name = idDivisor+1;

		// insert the place into the Trie for PlacesToId
		// we first insert into the trie in order to get the Place node that already exists if any
		// for this place, or the new one that was created with this insertion.
		// this way we will always get the same index for the same place name regardless of id
		TrieNode *insertedPlace = TrieInsert(PlacesToId, name, nameDivisor-name, id, places);
		// create a new Place structure only if this was a new Place and not an existing place with
		// a different id, like Asia or Brasil
		if( insertedPlace->placeId == id ){
			PlaceNodeStruct *node = new PlaceNodeStruct();
			node->id = id;
			node->index = insertedPlace->placeIndex;
			Places.push_back(node);
			places++;
		}
		// map the place id to the place index
		(*PlaceIdToIndex)[id] = insertedPlace->placeIndex;

		//printf("place[%ld] name[%*s] index[%ld] idToIndex[%ld]\n", id, nameDivisor-name, name,  insertedPlace->placeIndex, (*PlaceIdToIndex)[id]);

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


void readPlacePartOfPlace(char *inputDir){
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
	char *startLine = ((char*) memchr(buffer, '\n', 100)) + 1;
	char *EndOfFile = buffer + lSize;
	char *lineEnd;
	char *idDivisor;
	while (startLine < EndOfFile) {
		int len = EndOfFile - startLine;
		lineEnd = (char*) memchr(startLine, '\n', len);
		idDivisor = (char*) memchr(startLine, '|', len);
		*idDivisor = '\0';
		*lineEnd = '\0';
		long idA = atol(startLine);
		long idB = atol(idDivisor+1);

		if( idA != idB ){
			// insert the place idA into the part of place idB
			long indexA = (*PlaceIdToIndex)[idA];
			long indexB = (*PlaceIdToIndex)[idB];
			if( indexA != indexB ){
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


void readPersonLocatedAtPlace(char *inputDir){
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
	char *startLine = ((char*) memchr(buffer, '\n', 100)) + 1;
	char *EndOfFile = buffer + lSize;
	char *lineEnd;
	char *idDivisor;
	while (startLine < EndOfFile) {
		int len = EndOfFile - startLine;
		lineEnd = (char*) memchr(startLine, '\n', len);
		idDivisor = (char*) memchr(startLine, '|', len);
		*idDivisor = '\0';
		*lineEnd = '\0';
		long idPerson = atol(startLine);
		long idPlace = atol(idDivisor+1);

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
	for(unsigned long i=0; i<Places.size(); i++){
		c += Places[i]->personsThis.size();
		//printf("%ld - %ld\n", i, Places[i]->personsThis.size());
	}

	sprintf(msg, "Total persons located at place: %ld found inserted[%ld]", persons, c);
	printOut(msg);
#endif
}


void readOrgsLocatedAtPlace(char *inputDir){
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
	char *startLine = ((char*) memchr(buffer, '\n', 100)) + 1;
	char *EndOfFile = buffer + lSize;
	char *lineEnd;
	char *idDivisor;
	while (startLine < EndOfFile) {
		int len = EndOfFile - startLine;
		lineEnd = (char*) memchr(startLine, '\n', len);
		idDivisor = (char*) memchr(startLine, '|', len);
		*idDivisor = '\0';
		*lineEnd = '\0';
		long idOrg = atol(startLine);
		long idPlace = atol(idDivisor+1);

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

	// now we can delete PlaceId to Index hashmap since no further
	// data will come containing the PlaceId
	delete PlaceIdToIndex;
	PlaceIdToIndex = NULL;
}

void readPersonWorksStudyAtOrg(char *inputDir){
	char path[1024];
	char *paths[2] = {
		CSV_PERSON_STUDYAT_ORG,
		CSV_WORKAT_ORG
	};

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
		char *startLine = ((char*) memchr(buffer, '\n', 100)) + 1;
		char *EndOfFile = buffer + lSize;
		char *lineEnd;
		char *idDivisor;
		char *orgDivisor;
		while (startLine < EndOfFile) {
			int len = EndOfFile - startLine;
			lineEnd = (char*) memchr(startLine, '\n', len);
			idDivisor = (char*) memchr(startLine, '|', len);
			orgDivisor = (char*) memchr(idDivisor+1, '|', len);
			*idDivisor = '\0';
			*orgDivisor = '\0';
			long idPerson = atol(startLine);
			long idOrg = atol(idDivisor+1);

			// insert the place idA into the part of place idB
			long indexPlace = (*OrgToPlace)[idOrg];
			Places[indexPlace]->personsThis.push_back(idPerson);
			//printf("person[%ld] org[%ld] place[%ld]\n", idPerson, idOrg, indexPlace);

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

	}// end of file processing

	// safe to delete this vector since we do not need it anymore
	OrgToPlace->clear();
	delete OrgToPlace;
	OrgToPlace = NULL;
}

void readPersonHasInterestTag(char *inputDir){
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

	// process the whole file in memory
	// skip the first line
	char *startLine = ((char*) memchr(buffer, '\n', 100)) + 1;
	char *EndOfFile = buffer + lSize;
	char *lineEnd;
	char *idDivisor;
	while (startLine < EndOfFile) {
		int len = EndOfFile - startLine;
		lineEnd = (char*) memchr(startLine, '\n', len);
		idDivisor = (char*) memchr(startLine, '|', len);
		*idDivisor = '\0';
		*lineEnd = '\0';
		long personId = atol(startLine);
		long tagId = atol(idDivisor+1);

		PersonToTags[personId].tags.push_back(tagId);
		//printf("%ld %ld\n", idA, idB);

		startLine = lineEnd + 1;
#ifdef DEBUGGING
		personHasTag++;
#endif
	}
	// close the comment_hasCreator_Person
	fclose(input);
	free(buffer);

	// sort the tags to make easy the comparison
	// TODO - create signatures for the tags insteads
	for( long i=0; i<N_PERSONS; i++ ){
		std::stable_sort(PersonToTags[i].tags.begin(), PersonToTags[i].tags.end());
	}

#ifdef DEBUGGING
	sprintf(msg, "Total person tags : %ld", personHasTag);
	printOut(msg);
#endif

}

void readTags(char* inputDir) {
	char path[1024];
	path[0] = '\0';
	strcat(path, inputDir);
	strcat(path, CSV_TAG);
	FILE *input;

	input = fopen(path, "r");
	if (input == NULL) {
		printErr("could not open tag.csv!");
	}
	setvbuf(input, NULL, _IOFBF, FILE_VBUF_SIZE);
	long lines = countFileLines(input);
	fclose(input);
	//long lines = countFileLines(path);
	N_TAGS = lines - 1;

#ifdef DEBUGGING
	char msg[100];
	sprintf(msg, "Total tags: %d", N_TAGS);
	printOut(msg);
#endif
}


///////////////////////////////////////////////////////////////////////
// QUERY EXECUTORS
///////////////////////////////////////////////////////////////////////

void query1(int p1, int p2, int x){
	//printf("query1: %d %d %d\n", p1, p2, x);

	char *visited = (char*)malloc(N_PERSONS);
	memset(visited, 0, N_PERSONS);
	vector<QueryBFS> Q;

	// insert the source node into the queue
	Q.push_back(QueryBFS(p1, 0));
	unsigned long index=0;
	unsigned long size = 1;
	while( index < size ){
		QueryBFS current = Q[index];
		index++;

		//printf("current: %ld %d\n", current.person, current.depth);
		// mark node as visited - BLACK
		visited[current.person] = 2;

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
				if (visited[cAdjacent] == 0) {
					if (cAdjacent == p2) {
						Answers1.push_back(current.depth + 1);
						free(visited);
						return;
					}
					visited[cAdjacent] = 1;
					//&& cPerson->adjacentPersonWeights[cAdjacent] > x) {
					Q.push_back(QueryBFS(cAdjacent, current.depth+1));
					size++;
				}
			}
		} else {
			// no comments limit
			for (long i = 0, sz = cPerson->adjacents; i < sz; i++) {
				long cAdjacent = adjacents[i];
				// if node not visited and not added
				if (visited[cAdjacent] == 0) {
					if (cAdjacent == p2) {
						Answers1.push_back(current.depth + 1);
						free(visited);
						return;
					}
					// mark node as added - GREY
					visited[cAdjacent] = 1;
					Q.push_back(QueryBFS(cAdjacent, current.depth+1));
					size++;
				}
			}
		} // end of neighbors processing
	}

	free(visited);
	// no path found
	Answers1.push_back(-1);
}

int BFS_query3(long idA, long idB, int h){
	char *visited = (char*)malloc(N_PERSONS);
	memset(visited, 0, N_PERSONS);

	vector<QueryBFS> Q;
	long qIndex=0;
	long qSize=1;
	Q.push_back(QueryBFS(idA, 0));
	while( qIndex < qSize ){
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
		visited[cPerson.person] = 2;

		long *neighbors = Persons[cPerson.person].adjacentPersonsIds;
		for( long i=0,sz=Persons[cPerson.person].adjacents; i<sz; i++ ){
			long cB = neighbors[i];
			// if person is not visited and not added yet
			if( visited[cB] == 0 ){
				// check if this is our person
				if( idB == cB ){
					free(visited);
					return cPerson.depth + 1;
				}
				// mark person as GREY - added
				visited[cB] = 1;
				Q.push_back(QueryBFS(cB, cPerson.depth+1));
				qSize++;
			}
		}
	}
	free(visited);
	return INT_MAX;
}

void query3(int k, int h, char *name, int name_sz){
	//printf("query3 k[%d] h[%d] name[%*s] name_sz[%d]\n", k, h, name_sz, name, name_sz);

	vector<long> persons;
	vector<bool> visitedPersons;
	visitedPersons.resize(N_PERSONS);

	// get all the persons that are related to the place passed in
	char *visitedPlace = (char*)malloc(Places.size());
	memset(visitedPlace, 0, Places.size());
	TrieNode *place = TrieFind(PlacesToId, name, name_sz);
	long index = place->placeIndex;
	vector<long> Q_places;
	Q_places.push_back(index);
	// set as added
	visitedPlace[index] = 1;
	long qIndex = 0;
	long qSize = 1;
	while(qIndex < qSize){
		long cPlace = Q_places[qIndex];
		qIndex++;
		// set visited
		visitedPlace[cPlace] = 2;
		PlaceNodeStruct *cPlaceStruct = Places[cPlace];
		//std::copy (cPlaceStruct->personsThis.begin(),cPlaceStruct->personsThis.end(),std::back_inserter(persons));
		std::vector<long>::iterator cPerson=cPlaceStruct->personsThis.begin();
		std::vector<long>::iterator end=cPlaceStruct->personsThis.end();
		persons.reserve(persons.size()+(end-cPerson));
		for( ; cPerson != end; cPerson++ ){
			if( visitedPersons[*cPerson] )
				continue;
			visitedPersons[*cPerson] = true;
			persons.push_back(*cPerson);
		}

		for (std::vector<long>::iterator it = cPlaceStruct->placesPartOfIndex.begin();
				it != cPlaceStruct->placesPartOfIndex.end(); ++it){
			// if not visited
			if( visitedPlace[*it] == 0 ){
				// set as added
				visitedPlace[*it] = 1;
				Q_places.push_back(*it);
				qSize++;
				//printf("added place[%ld] of [%ld]persons\n", *it, Places[*it]->personsThis.size() );
			}
		}
	}

	//printf("found for place [%*s] persons[%ld] index[%ld]\n", name_sz, name, persons.size(), index);

	// now we have all the required persons so we have to calculate the common tags
	// for each pair and insert them into the priority queue in order to get the maximum K later
	priority_queue<Query3PQ, vector<Query3PQ>, Query3PQ_Comparator> PQ;
	for( long i = 0,end=persons.size()-1; i<end; ++i ){
		long idA = persons[i];
	//for( std::vector<long>::iterator idA = persons.begin(),end=persons.begin()+persons.size()-1; idA != end ; ++idA ){
		//for( std::vector<long>::iterator idB = idA+1; idB != persons.end(); ++idB ){
		for( long j = i+1, endd=persons.size(); j<endd; ++j ){
			long idB = persons[j];
			// WE DO NOT HAVE TO CHECK THESE PEOPLE IF THEY ARE NOT IN THE SAME SUBGRAPH
			if( Persons[idA].subgraphNumber != Persons[idB].subgraphNumber )
				continue;
			// we now have to calculate the common tags between these two people
			int cTags = 0;
			vector<long> &tagsA = PersonToTags[idA].tags;
			vector<long> &tagsB = PersonToTags[idB].tags;
			std::vector<long>::const_iterator iA = tagsA.begin();
			std::vector<long>::const_iterator endA = tagsA.end();
			std::vector<long>::const_iterator iB = tagsB.begin();
			std::vector<long>::const_iterator endB = tagsB.end();
			for( ; iA != endA && iB != endB ; ){
				if( *iA < *iB  )
					iA++;
				else if ( *iB < *iA )
					iB++;
				else if ( *iA == *iB ){
					cTags++;
					iA++;
					iB++;
				}
			}
			//printf("idA[%ld] idB[%ld] common[%ld]\n", idA, idB, cTags);
			if( idA <= idB ){
				PQ.push(Query3PQ(idA, idB, cTags));
			}else{
				PQ.push(Query3PQ(idB, idA, cTags));
			}
		}
	}

	// now we have to pop the K most common tag pairs
	// but we also have to check that the distance between them
	// is below the H-hops needed by the query.
	std::stringstream ss;
	for( ; k>0; k-- ){
		long idA = -1;
		long idB = -1;
		long cTags = -1;
		while( !PQ.empty() ){
			idA = PQ.top().idA;
			idB = PQ.top().idB;
			cTags = PQ.top().commonTags;
			PQ.pop();
			int distance = BFS_query3(idA, idB, h);
			if( distance <= h ){
				// we have an answer so exit the while
				break;
			}
		}
		//ss << idA << "|" << idB << "[" << cTags << "] ";
		ss << idA << "|" << idB << " ";
	}
	Answers3.push_back(ss.str());
}

///////////////////////////////////////////////////////////////////////
// MAIN PROGRAM
///////////////////////////////////////////////////////////////////////

void _initializations(){
	//CommentToPerson.reserve(1<<10);
	//PlaceIdToIndex.reserve(2048);
	CommentToPerson = new MAP_INT_INT();
	PlaceIdToIndex = new MAP_INT_INT();
	OrgToPlace = new MAP_INT_INT();

	PlacesToId = TrieNode_Constructor();
	Places.reserve(2048);


	Answers1.reserve(2048);
	Answers3.reserve(2048);
}

void _destructor(){
	delete[] Persons;
	delete[] PersonToTags;
	TrieNode_Destructor(PlacesToId);
}

void executeQueries(char *queriesFile){
	///////////////////////////////////////////////////////////////////
	// READ THE QUERIES
	///////////////////////////////////////////////////////////////////
	char path[1024];
	path[0] = '\0';
	strcat(path, queriesFile);
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
		lineEnd = (char*) memchr(startLine, '\n', 100);

		int queryType = atoi(startLine+5);
		switch( queryType ){
		case 1:
		{
			char *second = ((char*) memchr(startLine+7, ',', 20)) + 1;
			*(second-1) = '\0';
			char *third = ((char*) memchr(second, ',', 20)) + 1;
			*(lineEnd-1) = '\0';
			query1(atoi(startLine+7), atoi(second), atoi(third));
			break;
		}
		case 3:
		{
			char *second = ((char*) memchr(startLine + 7, ',', 20)) + 1;
			*(second - 1) = '\0';
			char *third = ((char*) memchr(second, ',', 20)) + 1;
			*(third - 1) = '\0';
			*(lineEnd - 1) = '\0';
			char *name = third+1; // to skip one space
			query3(atoi(startLine + 7), atoi(second), name, lineEnd-1-name);
			break;
		}
		default:
		{
			*lineEnd = '\0';
			//printOut(startLine);
		}
		}

		startLine = lineEnd+1;
	}
	free(buffer);
}

int main(int argc, char** argv) {
/*
	inputDir = argv[1];
	queryFile = argv[2];
*/

	// MAKE GLOBAL INITIALIZATIONS
	char msg[100];
	_initializations();

	long long time_global_start = getTime();
	/////////////////////////////////
	readPersons(inputDir);
	readPersonKnowsPerson(inputDir);

#ifdef DEBUGGING
	long time_persons_end = getTime();
	sprintf(msg, "persons graph time: %ld", time_persons_end - time_global_start);
	printOut(msg);
#endif

	readComments(inputDir);
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

	readPersonHasInterestTag(inputDir);

#ifdef DEBUGGING
	long time_places_end = getTime();
	sprintf(msg, "places process time: %ld", time_places_end - time_global_start);
	printOut(msg);
#endif


	executeQueries(queryFile);

#ifdef DEBUGGING
	long time_queries_end = getTime();
	sprintf(msg, "queries process time: %ld", time_queries_end - time_global_start);
	printOut(msg);

	/////////////////////////////////
	long long time_global_end = getTime();
	sprintf(msg, "Total time: micros[%lld] seconds[%.6f]",
			time_global_end - time_global_start,
			(time_global_end - time_global_start) / 1000000.0);
	printOut(msg);


#endif

	for(int i=0, sz=Answers1.size(); i<sz; i++){
		//printf("answer %d: %d\n", i, Answers1[i]);
		printf("%d\n", Answers1[i]);
	}

	for(int i=0, sz=Answers3.size(); i<sz; i++){
		//printf("answer %d: %s\n", i, Answers3[i].c_str());
		printf("%s\n", Answers3[i].c_str());
	}

	_destructor();
}



////////////////////////////////////////////////////////////////////////
// TRIE IMPLEMENTATION
////////////////////////////////////////////////////////////////////////
TrieNode* TrieNode_Constructor(){
	TrieNode* n = (TrieNode*)malloc(sizeof(TrieNode));
	if( !n ) printErr("error allocating TrieNode");
	n->valid = 0;
	memset( n->children, 0, VALID_PLACE_CHARS*sizeof(TrieNode*) );
	return n;
}
void TrieNode_Destructor( TrieNode* node ){
	for( int i=0; i<VALID_PLACE_CHARS; i++ ){
		if( node->children[i] != 0 ){
			TrieNode_Destructor( node->children[i] );
		}
	}
	free( node );
}
TrieNode* TrieInsert( TrieNode* node, const char* name, char name_sz, long id, long index){
	char ptr=0;
	int pos;
	while( ptr < name_sz ){
		//pos=name[ptr]-'a';
		pos = (unsigned char)name[ptr];
		if( node->children[pos] == 0 ){
			node->children[pos] = TrieNode_Constructor();
		}
		node = node->children[pos];
		ptr++;
	}
	// if already exists we do not overwrite but just return the existing one
	if( 1 == node->valid ){
		return node;
	}
	node->valid = 1;
	node->placeId = id;
	node->placeIndex = index;
	return node;
}
TrieNode* TrieFind( TrieNode* root, const char* name, char name_sz ){
	int p, i, found=1;
	for( p=0; p<name_sz; p++ ){
		//i = word[p] -'a';
		i = (unsigned char)name[p];
		if( root->children[i] != 0 ){
			root = root->children[i];
		}else{
			found=0;
			break;
		}
	}
	if( found && root->valid ){
		// WE HAVE A MATCH SO return the node
		return root;
	}
	return 0;
}



