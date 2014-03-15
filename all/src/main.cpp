/*
 * main.cpp
 *
 *  Created on: Mar 14, 2014
 *      Author: lambros
 */

#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <sys/time.h>

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
	}
	//LIST_INT adjacentPersons;
	long *adjacentPersonsIds;
	long adjacents;

	MAP_INT_INT commentsToPerson;

	int subgraphNumber;
};

struct Query1BFS{
	long person;
	int depth;
};

///////////////////////////////////////////////////////////////////////////////
// GLOBAL STRUCTURES
///////////////////////////////////////////////////////////////////////////////
char *inputDir = "all/input/outputDir-1k";
char *queryFile = "all/queries/1k-queries.txt";

char *CSV_PERSON = "/person.csv";
char *CSV_PERSON_KNOWS_PERSON = "/person_knows_person.csv";
char *CSV_COMMENT_HAS_CREATOR = "/comment_hasCreator_person.csv";
char *CSV_COMMENT_REPLY_OF_COMMENT = "/comment_replyOf_comment.csv";

long N_PERSONS = 0;

PersonStruct *Persons;
MAP_INT_INT CommentToPerson;

vector<int> Answers1;

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
				PersonStruct *person = &Persons[idA];
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
		CommentToPerson[idA] = idB;

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
		lineEnd = (char*) memchr(startLine, '\n', 100);
		idDivisor = (char*) memchr(startLine, '|', lineEnd - startLine);
		*idDivisor = '\0';
		*lineEnd = '\0';
		long idA = atol(startLine);
		long idB = atol(idDivisor + 1);

		// we have to hold the number of comments between each person
		long personA = CommentToPerson[idA];
		long personB = CommentToPerson[idB];

		// increase the counter for the comments from A to B
		Persons[personA].commentsToPerson[personB]++;

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
	// TODO - SORT THE EDGES BASED ON THE COMMENTS from A -> B
	// TODO - Leave only the min(comments A-to-B, comments B-to-A) at each edge
	///////////////////////////////////////////////////////////////////


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
	source.depth = -1;
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
			if( x!=-1 ){
				for (long i = 0, sz = cPerson->adjacents; i < sz; i++) {
					long cAdjacent = adjacents[i];
					if (!visited[cAdjacent]
						&& cPerson->commentsToPerson[cAdjacent] > x
						&& Persons[cAdjacent].commentsToPerson[current.person] > x) {
						Query1BFS valid;
						valid.depth = current.depth + 1;
						valid.person = cAdjacent;
						Q.push_back(valid);
					}
				}
			} else {
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


///////////////////////////////////////////////////////////////////////
// MAIN PROGRAM
///////////////////////////////////////////////////////////////////////

void _initializations(){
	//CommentToPerson.reserve(1<<10);
	Answers1.reserve(2048);
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
		default:
		{
			*lineEnd = '\0';
			printOut(startLine);
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

	executeQueries(queryFile);

#ifdef DEBUGGING
	long time_queries_end = getTime();
	sprintf(msg, "queries process time: %ld", time_queries_end - time_global_start);
	printOut(msg);

	for(int i=0, sz=Answers1.size(); i<sz; i++){
		printf("answer %d: %d\n", i, Answers1[i]);
	}
#endif

	/////////////////////////////////
	long long time_global_end = getTime();
	sprintf(msg, "Total time: micros[%lld] seconds[%.6f]",
			time_global_end - time_global_start,
			(time_global_end - time_global_start) / 1000000.0);
	printOut(msg);
}
