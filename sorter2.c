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

int main (int argc, char* argv[]) {
	//processes,pids, and original parent
	//storing children ids 
	pid_t* childPids = (pid_t*)(malloc(sizeof(pid_t) * 256));
	
	//check inputs 
	char *errorMessage = "The command line must follow either:\n./sorter -c  valid_column -d inputdir -o outputdir\n./sorter -c  valid_column -d inputdir\n./sorter -c  valid_column";
	if(strcmp(argv[1],"-c") !=0){
		printf("%s",errorMessage);
		exit(1);
	}

	char* column_to_sort = argv[2];

	if(argc == 3){ //Default behavior is search the current directory
		travdir(childPids, "./", column_to_sort, NULL);
	} else if (argc == 5){
		if (strcmp(argv[3],"-d") != 0) {
			printf("%s",errorMessage);
			exit(1);
		} else {
			//time to check if this is a csv file or a directory
			travdir(childPids, argv[4], column_to_sort, NULL);
		}
	} else if (argc == 7){
		if (strcmp(argv[3],"-d") != 0 && strcmp(argv[5],"-o") != 0) {
			printf("%s",errorMessage);
			exit(1);
		} else {
			//time to check if this is a csv file or a directory
			//-o output csv files to a certain directory thatdir if they specify -> argv[6] is output directory
			travdir(childPids, argv[4], column_to_sort, argv[6]);
		}
	} else {
		printf("%s",errorMessage);
		exit(1);		
	}

	return 0;
}

//traverse the directory for a csv 
int travdir (pid_t* childPids, const char * input_dir_path, char* column_to_sort, const char * output_dir)
{
	char *directory_path = (char *) malloc(MAX_PATH_LENGTH);
	strcpy(directory_path, input_dir_path);
	
	printf("opening first directory : %s\n",directory_path);
	//counter counts how many child processes have been created
	int counter = 0;
	DIR * directory = opendir(directory_path);

	if (directory == NULL) {
        printf ("Not a valid directory'%s'\n", directory_path);
        return 1;
	}
	
	//while we go through the directory -> in the parent process keep looking for csv files
	while(directory != NULL) {
		struct dirent * currEntry;
		char * d_name;
		currEntry = readdir(directory);
		printf("I am in a directory, it got here\n");

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
				counter++;
				//need to EXTEND THE PATH for next travdir call, working dir doesn't change (think adir/ -> adir/bdir/....)
				int pathlength = 0;	
				char path[MAX_PATH_LENGTH];
				pathlength = snprintf(path, MAX_PATH_LENGTH, "%s/%s",currEntry, d_name);
				if(pathlength > MAX_PATH_LENGTH-1) {
					printf("Path length is too long error");
					return -4;
				}
				//open new directory again
				pid_t pid,ppid;
				//fork returns 0 if child process
				pid = fork();
				//ppid = getppid();

				if (pid < 0) {
          			printf("Failed to fork process 1\n");
         			break;
     			}
				else if(pid==0){ //if child then go to newpath
					//Add on the d_name to the directory_
					printf("This is the child process searching through next directory. My pid is %d and my parent's id is %d.\n", getpid(), getppid());
					strcat(directory_path,"/");
					strcat(directory_path,d_name);
					directory = opendir(directory_path);
				}
				else if(pid>0){ //parent
					printf("This is the parent process. My pid is %d and my parent's id is %d.\n", getpid(), pid);
                    pid_t child_id;
                    child_id = getpid();
                    //store child process
                    childPids[counter]=child_id;
                    printf( "%sparent forked new child \n", child_id);
				}
			}
		} 
		else if(currEntry->d_type == DT_REG) { 	//regular files, need to check to ensure ".csv"
			//need a for loop to go through the directory to get all csvs - pointer and travdir once at the end of the list of csvs in that one dir	
			printf("checked that the csv is a file\n");
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
				//IF THE FILE CONTAINS 
				counter++;
				//specify to what directory openddir and then do d_name
				printf("File not null\n");

				pid_t ppid,pid;
				//fork returns 0 if child process
				pid = fork();

				if (pid < 0) {
          			printf("Failed to fork process 1\n");
         			break;
     			}
				//copies the code that you are running and returns zero to a new pid 
				else if(pid == 0){ //if child then sort
					printf("This is the child process. My pid is %d and my parent's id is %d.\n", getpid(), getppid());
					//change path for accessing the csv file
					char * csvFileOutputPath = malloc(sizeof(char)*512);
					//Remove the ".csv" from d_name to insert "-sorted-VALIDCOLUMN.csv
					char *file_name = (char *) malloc(strlen(d_name) + 1);
					strcpy(file_name, d_name);					
					char *lastdot = strrchr(file_name, '.');
					if (lastdot != NULL){
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
					
					printf("pathname: %s\n", csvFileOutputPath);
					printf("d_name: %s\n",d_name);
					char * path;

					FILE *csvFileOut = fopen(csvFileOutputPath,"w");

					sortnew(csvFile, csvFileOut, column_to_sort);
					printf("path that child spit out sort process to%s\n", path);
				
				} else if(pid>0) { //parent
					printf("This is the parent process. My pid is %d and my parent's id is %d.\n", getpid(), pid);
					pid_t child_id;
					child_id = pid; 
					//store child process
					childPids[counter]=child_id;
				}
			//wait after parent is done looking for csvs and THEN wait for children to end
			//once the child is done fork will return 0 
			} else {
				printf("File has already been sorted or is NULL: %s\n", d_name);
			}
		} else {
			fprintf(stderr, "ERROR: Input is not a file or a directory\n");
			exit(1);
		}	
	}
	//WAIT FOR CHILDREN TO FINISH 
	//go through children array -> CHILDREN THAT HAVENT FINISHED YET
	int j;
	int child_status;
	pid_t child_ids_waiting_on;
	int totalprocesses = counter;
	printf("totalprocesses before waiting: %d\n",totalprocesses);
	//childPids[counter]; 

	printf("total number of processes created %d\n", counter);	

	while(counter >= 0){
		//wait() is used to wait until any one child process terminates
		//wait returns process ID of the child process for which status is reported
		child_ids_waiting_on = wait(&child_status);
		//if((childPids[j] = wait(child_status, WNOHANG)) > 0){
			//printf("%s parent caught SIGCHLD from\n", childPids[j]);
			//children.remove(pid); //this isnt right this is python change to C language
		//}
		--counter;
	}
	printf("closing directory: %s\n", directory_path); //DEBUGGING 
	if(closedir(directory)){
		printf("error could not close dir");
		return -3;
	}
	//outputMetadata(childPids,totalprocesses);
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
		//free(compareString);		
		return 0;
	} else {
		//free(compareString);
		return 1;		
	}

}

/*//Your code's output will be a series of new CSV files outputted to the file whose name is the name of the CSV file sorted, with "-sorted-<fieldname>" postpended to the name.
int outputMetadata(pid_t* childPids, int totalprocesses){
	int i;
	for(i=0;i<totalprocesses;i++){
		printf("%sOne child pid is\n", childPids[i]);
  		free(&childPids[i]);
	}
	free(childPids);
	printf("total number of processes: %d\n", totalprocesses+1);

	return 0;
}
*/

