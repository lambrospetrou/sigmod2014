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

#include <string>
#include <vector>
#include <map>

#include <tr1/unordered_map>

using namespace std;
using std::tr1::unordered_map;
using std::tr1::hash;

#define JOIN(x, y) JOIN_AGAIN(x, y)
#define JOIN_AGAIN(x, y) x ## y

#define DEBUGGING 1
#define FILE_VBUF_SIZE 1024

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
	long placeId;
	long placeIndex;
	char valid;
	TrieNode* children[VALID_PLACE_CHARS];
};

struct PlaceNodeStruct{
	long id;
	long index;
	vector<int> personsThis;
	vector<int> placesPartOfIndex;
};

/////////////////////////////
// QUERY SPECIFIC
/////////////////////////////
struct Query1BFS{
	long person;
	int depth;
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

long N_PERSONS = 0;

PersonStruct *Persons;
TrieNode *PlacesToId;
vector<PlaceNodeStruct*> Places;

vector<int> Answers1;
vector<string> Answers3;

// the two structures below are only used as intermediate steps while
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

long getFileSize(FILE *file){
	fseek(file, 0, SEEK_END);
	long lSize = ftell(file);
	rewind(file);
	return lSize;
}

//////////////////////////////////////////////////////////////////////////////

void readPersonKnowsPerson(FILE *input) {
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
	vector<int> ids;
	ids.reserve(128);
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
				for( int i=0,sz=ids.size(); i<sz; i++ ){
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

#ifdef DEBUGGING
	char msg[100];
	sprintf(msg, "Total edges: %d", edges);
	printOut(msg);
#endif

	free(buffer);
}

void readPersons(char* inputDir) {
	char path[1024];
	path[0] = '\0';
	strcat(path, inputDir);
	strcat(path, CSV_PERSON);
	FILE *input = fopen(path, "r");
	if (input == NULL) {
		printErr("could not open person.csv!");
	}
	setvbuf(input, NULL, _IOFBF, FILE_VBUF_SIZE);
	long lines = countFileLines(input);
	fclose(input);
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

	path[0] = '\0';
	strcat(path, inputDir);
	strcat(path, CSV_PERSON_KNOWS_PERSON);
	input = fopen(path, "r");
	if (input == NULL) {
		printErr("could not open person_knows_person!");
	}
	setvbuf(input, NULL, _IOFBF, FILE_VBUF_SIZE);

	// import the edges
	readPersonKnowsPerson(input);

	fclose(input);
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
			//qsort(Persons[i].adjacentPersonsIds, N_PERSONS, sizeof(long), PersonCommentsComparator);
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
	// since we have all the data needed in arrays we can delete the hashmaps
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
		PlaceNodeStruct *node = new PlaceNodeStruct();
		node->id = id;
		node->index = places;
		Places.push_back(node);
		(*PlaceIdToIndex)[id] = places;
		TrieInsert(PlacesToId, name, nameDivisor-name, id, places);

		//printf("%d %s\n", id, name);

		startLine = lineEnd + 1;
#ifdef DEBUGGING
		places++;
#endif
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
			Places[indexA]->placesPartOfIndex.push_back(indexB);
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
		//printf("%ld %ld\n", idA, idB);

		startLine = lineEnd + 1;
#ifdef DEBUGGING
		persons++;
#endif
	}
	// close the comment_hasCreator_Person
	fclose(input);
	free(buffer);

#ifdef DEBUGGING
	sprintf(msg, "Total persons located at place: %ld", persons);
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
		//printf("%ld %ld\n", idA, idB);

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
			printErr("could not open person ToOrganisation file!");
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
			long idOrg = atol(orgDivisor);

			// insert the place idA into the part of place idB
			long indexPlace = (*OrgToPlace)[idOrg];
			Places[indexPlace]->personsThis.push_back(idPerson);
			//printf("%ld %ld\n", idA, idB);

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

///////////////////////////////////////////////////////////////////////
// QUERY EXECUTORS
///////////////////////////////////////////////////////////////////////

void query1(int p1, int p2, int x){
	//printf("query1: %d %d %d\n", p1, p2, x);

	char *visited = (char*)malloc(N_PERSONS);
	memset(visited, 0, N_PERSONS);
	vector<Query1BFS> Q;

	// insert the source node into the queue
	Query1BFS source;
	source.depth = 0;
	source.person = p1;
	Q.push_back(source);
	unsigned long index=0;
	while( index < Q.size() ){
		Query1BFS current = Q[index];
		index++;
		if( visited[current.person] ){
			continue;
		}
		//printf("current: %ld %d\n", current.person, current.depth);
		visited[current.person] = 1;

		if( current.person == p2 ){
			Answers1.push_back(current.depth);
			free(visited);
			return;
		}else{
			// we must add the current neighbors into the queue if
			// the comments are valid
			PersonStruct *cPerson = &Persons[current.person];
			long *adjacents = cPerson->adjacentPersonsIds;
			long *weights = cPerson->adjacentPersonWeightsSorted;
			// if there is comments limit
			if( x!=-1 ){
				for (long i = 0, sz = cPerson->adjacents; (i < sz) && (weights[i] > x); i++) {
					long cAdjacent = adjacents[i];
					if (!visited[cAdjacent] ){
					    //&& cPerson->adjacentPersonWeights[cAdjacent] > x) {
						Query1BFS valid;
						valid.depth = current.depth + 1;
						valid.person = cAdjacent;
						Q.push_back(valid);
					}
				}
			} else {
				// no comments limit
				for (long i = 0, sz = cPerson->adjacents; i < sz; i++) {
					long cAdjacent = adjacents[i];
					if( !visited[cAdjacent] ){
						Query1BFS valid;
						valid.depth = current.depth + 1;
						valid.person = cAdjacent;
						Q.push_back(valid);
					}
				}
			} // end of neighbors processing
		} // end if not current node is the destination
	}

	free(visited);
	// no path found
	Answers1.push_back(-1);
}

void query3(int k, int h, char *name, int name_sz){
	printf("query3 k[%d] h[%d] name[%*s] name_sz[%d]\n", k, h, name_sz, name, name_sz);
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
	//if( argc != 1 ){
	//if( argc != 3 ){
	//printErr("Wrong number of arguments. ./binary input_dir output_dir");
	//}

	// MAKE GLOBAL INITIALIZATIONS
	char msg[100];
	_initializations();

	long long time_global_start = getTime();
	/////////////////////////////////
	readPersons(inputDir);
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
/*
	for(int i=0, sz=Answers1.size(); i<sz; i++){
		printf("answer %d: %d\n", i, Answers1[i]);
	}
*/
#endif

	/////////////////////////////////
	long long time_global_end = getTime();
	sprintf(msg, "Total time: micros[%lld] seconds[%.6f]",
			time_global_end - time_global_start,
			(time_global_end - time_global_start) / 1000000.0);
	printOut(msg);

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



