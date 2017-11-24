#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <ctype.h>
#include <errno.h>
#include <dirent.h>
#include <pthread.h>
#include "sorterthreading.c"
#include "sorter2.h"

#define MAX_PATH_LENGTH 256

//counter counts how many child processes have been created
int counter = 0;

//arguments for processfiletosort
//Each thread allocates memory for these arguments, and are used throughout the sorting process.
typedef struct threadArg{
	char* fileName;
	char* pathName;
	char* directoryName;
	FILE* csvFile;
	char* column_to_sort;
	char* directory_path;
	char* output_dir;
	int threadNumber;
} arg;

//arguments for travdir
typedef struct threadArg2{
	pthread_t* threadHolder;
	DIR* directory;
	char* directory_path;
	char* column_to_sort;
} arg2;

int main (int argc, char* argv[]) {
	//check inputs 
	char* column_to_sort="";
	char* starting_dir="";
	char* output_dir="";

	char *errorMessage = "The command line must specify a column to sort with arg -c \nfor example:\n./sorter -c  valid_column -d inputdir -o outputdir\n./sorter -c  valid_column -d inputdir\n./sorter -c  valid_column\n";
	int i; 
	for (i = 0; i < argc; i++) { 
		//printf("%s\n", argv[i]); 
		char* argument = argv[i];
		if(strcmp(argument,"-d") == 0){
			starting_dir = argv[i+1];
		} else if(strcmp(argument,"-c") == 0) {
			column_to_sort = argv[i+1];
		} else if(strcmp(argument,"-o") == 0){
			output_dir = argv[i+1];
		}
	}

	if(*column_to_sort==0){ 
		printf("%s",errorMessage);
		exit(1);
	} else if (*starting_dir==0){
		//check for an output directory
		if(*output_dir==0){
			//time to check if this is a csv file or a directory
			travdir("./", column_to_sort, NULL);
		}
		else{
			//there is a valid -c and valid -o parameter 
			travdir("./", column_to_sort, output_dir);
		}
	} else if (*output_dir==0){ //Default behavior is search the current directory - if output_dir is null
		//assume that starting dir is not null
		travdir(starting_dir, column_to_sort, NULL);
	} else {
		//printf("all possible exist %s, %s, %s\n", column_to_sort, starting_dir, output_dir);
		travdir(starting_dir, column_to_sort, output_dir);	
	}
	return 0;
}

//Takes in a thread argument structure, and uses that to retrieve all necessary information.
void processFiletoSort(void* args){
	//THOUGHTS: MAYBE OPEN THE CSVFILE HERE AND NOT IN TRAVDIR ????
	pthread_id_np_t tid;
	tid = pthread_getthreadid_np();
	printf("The tid of this thread is: %s \n", tid);

	//change path for accessing the csv file
	char * csvFileOutputPath = malloc(sizeof(char)*512);
	//Remove the ".csv" from d_name to insert "-sorted-VALIDCOLUMN.csv
	char *file_name = (char *) malloc(strlen(args->directoryName) + 1);
	strcpy(file_name, args->directoryName);					
	char *lastdot = strrchr(file_name, '.');
	if (lastdot != NULL) {
		*lastdot = '\0';
	}
					
	//Default behavior dumps files into input directory
	if(args->output_dir == NULL) {
		strcpy(csvFileOutputPath,args->directory_path);
	} else { //If given an output directory
		struct stat sb;
		if (stat(args->output_dir, &sb) == -1) {
			mkdir(args->output_dir, 0700); //RWX for owner
		} 
		strcpy(csvFileOutputPath,args->output_dir);
	}

	strcat(csvFileOutputPath,"/");
	strcat(csvFileOutputPath,args->fileName);
	strcat(csvFileOutputPath,"-sorted-");
	strcat(csvFileOutputPath,args->column_to_sort);
	strcat(csvFileOutputPath,".csv");

	FILE *csvFileOut = fopen(csvFileOutputPath,"w");

	//sort the csv file
	sortnew(args->csvFile, csvFileOut, args->column_to_sort);
	free(csvFileOutputPath);
	free(args->fileName);

	pthread_exit(NULL);
	return NULL;
}

//manages the created threads
void createThreadsSort(pthread_t* threadHolder, char* pathname, char* d_name, char* column_to_sort, FILE* csvFile, char* output_dir, char* directory_path, int counter){
	arg *args = calloc(1,sizeof *args);
	args->fileName = fileName;
	args->pathName = pathname;
	args->directoryName = d_name;
	args->csvFile = csvFile;
	args->column_to_sort = column_to_sort;
	args->directory_path = directory_path;
	args->output_dir = output_dir;
	args->threadNumber = counter;
	pthread_create(&threadHolder[threadNumber], 0, processFiletoSort, args);
	return;
}

//traverse the directory for a csv 
int travdir(const char * input_dir_path, char* column_to_sort, const char * output_dir)
{
	int k;
	char *directory_path = (char *) malloc(MAX_PATH_LENGTH);
	strcpy(directory_path, input_dir_path);
	
	DIR * directory = opendir(directory_path);

	if (directory == NULL) {
        return 1;
	}

	//initialize this to hold 5 threads for now
	//have one thread go through directories
	int numThreads = 5;
	pthread_t* threadHolder = (pthread_t*)(malloc(sizeof(pthread_t) * numThreads));

	args2->threadHolder = threadHolder;
	args2->directory = directory;
	args2->directory_path = directory_path;
	args2->column_to_sort = column_to_sort;
	goThroughPath(args2);

	return 0;
}
	
//function pointer to go through the path
void goThroughPath(void* args2){

	//copy from struct to this function
	char* column_to_sort = args2->column_to_sort;
	DIR* directory = args2->directory;
	char* directory = args2->directory;
	char* directory_path = args2->directory_path;

	//while we go through the directory -> in the parent process keep looking for csv files
	while(directory != NULL) {
		struct dirent * currEntry;
		char * d_name;
		currEntry = readdir(directory);

		//making sure not to fork bomb
		if(counter == 256){
			break;
		}
		
		//end of file stream, break-> now wait for children and children's children
		if(!currEntry) {
			break;
		}
		//d_name is the current directory/file
		d_name = currEntry->d_name;

		//this is a directory 
		if(currEntry->d_type==DT_DIR) {
			if(strcmp(d_name,".") != 0 && strcmp(d_name, "..") != 0) {
				//need to EXTEND THE PATH for next travdir call, working dir doesn't change (think adir/ -> adir/bdir/....)
				int pathlength = 0;	
				char path[MAX_PATH_LENGTH];
				pathlength = snprintf(path, MAX_PATH_LENGTH, "%s/%s",currEntry, d_name);
				if(pathlength > MAX_PATH_LENGTH-1) {
					printf("ERROR: Path length is too long");
					return;
				}
				//open new directory again
				childPids[counter] = child_id;
				strcat(directory_path,"/");
				strcat(directory_path,d_name);
				directory = opendir(directory_path);

				//use a function pointer to go through the travdir again
				args2->directory = directory;
				args2->directory_path = directory_path;
				args2->column_to_sort = column_to_sort;
				pthread_create(&threadHolder[threadNumber], 0, goThroughPath, args2);
				counter++;
				//THOUGHTS: where do we call pthread_exit? Do we need to do that?
			}
		} 
		else if(currEntry->d_type == DT_REG) { 	//regular files, need to check to ensure ".csv"
			//need a for loop to go through the directory to get all csvs - pointer and travdir once at the end of the list of csvs in that one dir	
			char pathname [256];
			FILE* csvFile;
			sprintf(pathname, "%s/%s", directory_path, d_name);
			csvFile = fopen(pathname, "r");

			//Check to see if the file is a csv
			char *lastdot = strrchr(d_name, '.');

			if (strcmp(lastdot,".csv") != 0) {
				printf("File is not a .csv: %s\n", d_name);
			} 
			else if (csvFile != NULL && !isAlreadySorted(pathname, column_to_sort)) {
				//pathname has the full path to the file including extension
				//directory_path has only the parent directories of the file
				//instead of forking we call the method createThreadsSort
				createThreadsSort(threadHolder, pathname, d_name, column_to_sort, csvFile, output_dir, directory_path, counter);
				counter++;
			}
		} else {
			fprintf(stderr, "ERROR: Input is not a file or a directory\n");
			return;
		}	
	}

	//WAIT FOR THREADS TO FINISH 
	int i = 0,j;
	int totalthreads = counter;

	while(totalthreads > 0){
		//pthread_join() is used to wait until any one child process terminates
		//wait returns process ID of the child process for which status is reported
		for (i = 0; i < 5; i++) {
			//need to print out the thread ids somehow here too
			pthread_join(threadHolder[i], NULL);
		}
		--totalthreads;
	}
	
	free(directory_path);	
	if(closedir(directory)){
		printf("ERROR: Could not close dir");
		return;
	}

	if(getpid() == root) {
		//this would return 5 rn
		printf("\nTotal number of threads: %d\n\r", (i + counter));	
	}
	exit(i);
	return;
}

//If it already contains the phrase -sorted-SOMEVALIDCOLUMN.csv then the file is already sorted
//Returns 0 if the file has not yet been sorted
//Returns 1 if the file has been sorted
int isAlreadySorted(char *pathname,char *column_to_sort) {
	char *compareString = (char *) malloc(strlen("-sorted-") + strlen(column_to_sort) + strlen(".csv") + 1);
	//build the -sorted-SOMEVALIDCOLUMN.csv string
	strcpy(compareString, "-sorted-");
	strcat(compareString, column_to_sort);
	strcat(compareString, ".csv");
	if(strstr(pathname,compareString) == NULL) {
		free(compareString);		
		return 0;
	} else {
		free(compareString);
		return 1;		
	}

}
