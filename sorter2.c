#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <ctype.h>
#include <errno.h>
#include <dirent.h>
#include <unistd.h>
#include <sys/wait.h>
#include "sorter.c"
#include "sorter2.h"

#define MAX_PATH_LENGTH 256

int counter = 0;
pid_t childPids[256];
pid_t root;

int main (int argc, char* argv[]) {
	//processes,pids, and original parent
	//storing children ids 
	
	//check inputs 
	char *errorMessage = "The command line must follow either:\n./sorter -c  valid_column -d inputdir -o outputdir\n./sorter -c  valid_column -d inputdir\n./sorter -c  valid_column";
	if(strcmp(argv[1],"-c") !=0){
		printf("%s",errorMessage);
		exit(1);
	}

	char* column_to_sort = argv[2];

	if(argc == 3){ //Default behavior is search the current directory
		travdir("./", column_to_sort, NULL);
	} else if (argc == 5){
		if (strcmp(argv[3],"-d") != 0) {
			printf("%s",errorMessage);
			exit(1);
		} else {
			//time to check if this is a csv file or a directory
			travdir(argv[4], column_to_sort, NULL);
		}
	} else if (argc == 7){
		if (strcmp(argv[3],"-d") != 0 && strcmp(argv[5],"-o") != 0) {
			printf("%s",errorMessage);
			exit(1);
		} else {
			//time to check if this is a csv file or a directory
			//-o output csv files to a certain directory thatdir if they specify -> argv[6] is output directory
			travdir(argv[4], column_to_sort, argv[6]);
		}
	} else {
		printf("%s",errorMessage);
		exit(1);		
	}

	return 0;
}

//traverse the directory for a csv 
int travdir (const char * input_dir_path, char* column_to_sort, const char * output_dir)
{
	int k;
	char *directory_path = (char *) malloc(MAX_PATH_LENGTH);
	strcpy(directory_path, input_dir_path);
	
	//counter counts how many child processes have been created
	DIR * directory = opendir(directory_path);

	if (directory == NULL) {
        return 1;
	}
	
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
					return -4;
				}
				//open new directory again
				pid_t pid,ppid;
				//fork returns 0 if child process
				pid = fork();
				counter++;

				if (pid < 0) {
          			printf("ERROR: Failed to fork process 1\n");
         			break;
     			}
				else if(pid == 0){ //if child then go to newpath
					//Add on the d_name to the directory
					pid_t child_id = getpid();
					childPids[counter] = child_id;
					strcat(directory_path,"/");
					strcat(directory_path,d_name);
					directory = opendir(directory_path);
				}
				else if(pid>0){ //parent
					if(counter == 1) {
						root = getpid();
						printf("Initial PID: %d\n", root);
					}
				}
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
				//pathname has the full path to the file including extenstion
				//directory_path has only the parent directories of the file
				
				pid_t ppid,pid;
				//fork returns 0 if child process
				pid = fork();
				counter++;

				if (pid < 0) {
          			printf("Failed to fork process 1\n");
         			break;
     			}
				//copies the code that you are running and returns zero to a new pid 
				else if(pid == 0){ //if child then sort
					childPids[counter] = getpid();
					//change path for accessing the csv file
					char * csvFileOutputPath = malloc(sizeof(char)*512);
					//Remove the ".csv" from d_name to insert "-sorted-VALIDCOLUMN.csv
					char *file_name = (char *) malloc(strlen(d_name) + 1);
					strcpy(file_name, d_name);					
					char *lastdot = strrchr(file_name, '.');
					if (lastdot != NULL) {
						*lastdot = '\0';
					}
					
					//Default behavior dumps files into input directory
					if(output_dir == NULL) {
						strcpy(csvFileOutputPath,directory_path);
					} else { //If given an output directory
						struct stat sb;
						if (stat(output_dir, &sb) == -1) {
							mkdir(output_dir, 0700); //RWX for owner
						} 
						strcpy(csvFileOutputPath,output_dir);
					}

					strcat(csvFileOutputPath,"/");
					strcat(csvFileOutputPath,file_name);
					strcat(csvFileOutputPath,"-sorted-");
					strcat(csvFileOutputPath,column_to_sort);
					strcat(csvFileOutputPath,".csv");

					FILE *csvFileOut = fopen(csvFileOutputPath,"w");

					sortnew(csvFile, csvFileOut, column_to_sort);
					free(csvFileOutputPath);
					free(file_name);
					exit(1);
				} else if(pid > 0) { //parent
					if(counter == 1) {
						root = getpid();
						printf("Initial PID: %d\n", root);
					}
				}
			//wait after parent is done looking for csvs and THEN wait for children to end
			//once the child is done fork will return 0 
			} else {

			}
		} else {
			fprintf(stderr, "ERROR: Input is not a file or a directory\n");
			exit(-1);
		}	
	}

	//WAIT FOR CHILDREN TO FINISH 
	int i = 0,j;
	int child_status;
	int totalprocesses = counter;

	while(totalprocesses > 0){
		//wait() is used to wait until any one child process terminates
		//wait returns process ID of the child process for which status is reported
		while( (j = wait(&child_status)) != -1) {
			printf("%d,", j);
			if(WIFEXITED(child_status)) {
				i = i + WEXITSTATUS(child_status);
			}
		}
		--totalprocesses;
	}
	
	free(directory_path);	
	if(closedir(directory)){
		printf("ERROR: Could not close dir");
		return -3;
	}

	if(getpid() == root) {
		printf("\nTotal number of processes: %d\n\r", (i + counter));	
	}
	exit(i);
}

//Will check the file name of th input file pointer.
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
