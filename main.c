/*********************************************
 * Class: CSC-415-02 Spring 2020
 * Name: Christian Pascual-Fernandez
 * Student ID: 916773824
 * Project: <WordBlast>
 *
 * File: <WordBlast>
 *
 * Description: This program takes a text file for input (in this case
 * we are using War and Peace), and returns the top 10 most frequently used
 * words that are of more than 6 characters long. This program implements multiple
 * threads that run simultaneously so as to complete the task in a short amount of time
 * by dividing the file into smaller, fairly equal parts.
 *
 * *******************************************************/



#include <stdio.h>
#include <pthread.h>
#include <time.h>
#include <string.h>
#include <stdlib.h>
#include <fcntl.h>
#include <zconf.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>


#ifdef DEBUG
#define DEBUG_TEST 1
#else
#define DEBUG_TEST 0
#endif

#define WL_SIZEBLOCK 2500
#define MINCHARS 6 //minimum number of characters must be 6
#define TOP 20 // number of top used words


#define debug_print(fmt, ...) \
    do { if(DEBUG_TEST) fprintf(stderr, fmt, __VA_ARGS__);} while (0)

pthread_mutex_t lock;

typedef struct chunkInfo {
    char * filename;
    int id;
    long start; //where to start in the file
    long size; //continue for how many bytes from chunk start
} chunkInfo, *chunkInfo_p;

typedef struct wordEntry {
    char *word;
    int count;
} wordEntry, * wordEntry_p;

volatile wordEntry_p wordList = NULL;   //array of words
volatile int wordListCount = 0;         //number of words in array
int maxWordListSize = 0;                //size of array

//adds word to frequency list
void addWord(char *word) {

    //is word already in the list?
    for(int i = 0; i < wordListCount; i++) {
        //if so: increment count by 1
        if(strcasecmp(wordList[i].word, word) == 0) { //0 means no difference between comparisons
            pthread_mutex_lock(&lock); // needed because wordList is a global array
            wordList[i].count = wordList[i].count + 1;
            pthread_mutex_unlock(&lock);
            return;
        }
    }
    //else: add word to list and set its count to 1
    //Word not found - add to list
    debug_print("Adding word %s\n", word);

    //do we have room for new word in list?
    pthread_mutex_lock(&lock);
    if(wordListCount >= maxWordListSize) { //need to reallocate, not enough room
        maxWordListSize = maxWordListSize + WL_SIZEBLOCK;

        if(wordList == NULL) {  //first time do malloc

            wordList = malloc(sizeof(wordEntry) * maxWordListSize);
            if (wordList == NULL) { //malloc failed
                printf("ERROR: Initial malloc of wordlist failed - trying to allocate %lu bytes\n", sizeof(wordEntry) *maxWordListSize );
                exit(-2);
            }
        } else  {               //other times to realloc
            wordEntry_p reallocRet = realloc(wordList, sizeof(wordEntry) * maxWordListSize);
            if (reallocRet == NULL) { //malloc failed
                printf("ERROR: Realloc of wordlist failed - trying to allocate %lu bytes\n", sizeof(wordEntry) *maxWordListSize );
                exit(-2);
            }
            wordList = reallocRet;

        }

    }
    //Add word to the list
    wordList[wordListCount].word = malloc(strlen(word) + 2);
    //should check if malloc succeeded...
    strcpy(wordList[wordListCount].word, word);
    wordList[wordListCount].count = 1;
    wordListCount++;

    pthread_mutex_unlock(&lock);
}





//characters to be ignored during tokenization
char *delim = "\"\'.""''?:;-,-*($%)! \t\n\x0A\r";

//Thread process for specific chunks of the file
void *processChunk(void * p) {
    chunkInfo_p info = (chunkInfo_p) p; //casting pointer to be a chunkInfo pointer
    char *buf;
    char *word;
    char *saveptr;
    int fileDescriptor;

    buf = malloc (info -> size + 4);
    if (buf == NULL) {
        printf("ERROR allocating a buffer in thread %d  (%ld bytes) \n", info->id, info->size + 4);
        return NULL;
    }

    fileDescriptor = open(info->filename, O_RDONLY);
    //make sure buffer becomes deallocated
    if (fileDescriptor == 0) {
        printf("ERROR opening %s in thread %d\n", info->filename, info->id);
        free(buf); //crucial to get out of error condition and if open fails
        return NULL;
    }

    lseek(fileDescriptor, info->start, SEEK_SET); //read from start point as directed by main
    long res = read(fileDescriptor, buf, info->size); //read this many bytes
    debug_print ("On %d Asked for %ld bytes, got %ld bytes\n", info->id, info->size, res);
    close(fileDescriptor);

    //null termination for safety, since data is being parsed like a large string
    buf[info->size] = 0;
    buf[info->size + 1] = 0; //basically protects buffers

    //strtok_r is thread safe, where strtok is not.
    word = strtok_r(buf, delim, &saveptr); //gets first word

    while (word != NULL) {
        if(strlen(word) >= MINCHARS) {
            debug_print("%d word is: %s\n", info->id, word);
            addWord(word);
        }
        word = strtok_r(NULL, delim, &saveptr); //gets all subsequent words
    }

    free(buf);
    return NULL;
}





int main(int argc, char *argv[]) {

    int fileDescriptor;
    int processCount;
    chunkInfo * infoArray;
    pthread_t * pt;
    char * filename;

    filename = "WarAndPeace.txt"; //default file unless specified
    processCount = 1;

    if(pthread_mutex_init(&lock, NULL) != 0) {
        printf("\n mutex init failed\n");
        return 1;
    }

    if (argc > 2) {
        filename = argv[1];
        processCount = atoi (argv[2]);
    }

    fileDescriptor = open(filename, O_RDONLY);

    //Getting file size
    lseek(fileDescriptor, 0, SEEK_END);
    long fileSize = lseek(fileDescriptor, 0, SEEK_CUR);
    lseek(fileDescriptor, 0, SEEK_SET); // needed for next read from beginning of file
    close(fileDescriptor);


    //timer requirement from assignment guidelines
    struct timespec startTime;
    struct timespec endTime;
    clock_gettime(CLOCK_REALTIME, &startTime);


    //allocate thread array info (array of chunkInfo's)
    infoArray = malloc (sizeof(chunkInfo) * processCount); //allocate chunkInfo to however many processes
    pt = malloc (sizeof(pthread_t) * processCount); //creating array of process thread IDs

    for (int i = 0; i < processCount; i++) {
        /*
         * Initialize parameters
         * Others will receive their own versions of this structure, rather than everyone
         * taking the same, overwritten structure
         * */
        infoArray[i].filename = filename;
        infoArray[i].id = i + 1;
        infoArray[i].start = (fileSize / processCount) * i;

        //for last ending allocation so that every last byte of file is accounted for
        if (i == processCount - 1) {
            infoArray[i].size = fileSize - infoArray[i].start;
        } else {
            infoArray[i].size = (fileSize / processCount);
        }

        //creates thread
        pthread_create(&(pt[i]), NULL, processChunk, &(infoArray[i]));
    }

    //wait for threads to finish
    for(int i = 0; i < processCount; i++) {
        pthread_join(pt[i], NULL);
    }

    //Find the TOP list
    wordEntry top10[TOP];
    for (int j = 0; j < TOP; j++) {
        top10[j].count = 0;
    }

    for (int j = 0; j < wordListCount; j++) {
        if(wordList[j].count > top10[TOP - 1].count) {  //goes into top 10
            debug_print("Building Top %d. adding %s %d\n", TOP, wordList[j].word, wordList[j].count);
            top10[TOP-1].word = wordList[j].word;
            top10[TOP-1].count = wordList[j].count;

            for(int k = TOP-2; k >= 0; k--) {
                if (wordList[j].count > top10[k].count) {
                    top10[k+1].word = top10[k].word;
                    top10[k+1].count = top10[k].count;
                    top10[k].word = wordList[j].word;
                    top10[k].count = wordList[j].count;
                }
            }


        }
    }

    //Print Headers
    printf("\n\nWord Frequency Count on %s with %d threads\n", filename, processCount);
    printf("Printing top %d words %d characters or more. \n", TOP, MINCHARS);

    //print top X
    for (int k = 0; k < TOP; k++) {
        printf("Number %d is %s with a count of %d\n", k+1, top10[k].word, top10[k].count);
    }


    // (CLOCK OUTPUT) timer requirement from assignment guidelines
    clock_gettime(CLOCK_REALTIME, &endTime);
    time_t sec = endTime.tv_sec - startTime.tv_sec;
    long n_sec = endTime.tv_nsec - startTime.tv_nsec;
    if (endTime.tv_nsec < startTime.tv_nsec) {
        --sec;
        n_sec = n_sec + 1000000000L;
    }


    printf("Total Time Was %ld.%09ld seconds\n", sec, n_sec);

    //cleanup
    for (int j = 0; j < wordListCount; j++) {
        free(wordList[j].word);
        wordList[j].word = NULL;
    }
    free (wordList);
    wordList = NULL;
    pthread_mutex_destroy(&lock);
}
