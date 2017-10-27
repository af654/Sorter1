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

int main (int argc, char* argv[])
{
	//initialize all variables
	char* column_to_sort; 
	//processes,pids, and original parent
	//storing children ids 
	pid_t* childPids = (pid_t*)(malloc(sizeof(pid_t) * 256));
	//work with this for now ./sorter -c  movie_title -d thisdir -o thatdir
	//also have to account for ./sorter -c  movie_title
	//also have to account for ./sorter -c  movie_title -d thisdir/thatdir
	//check inputs 
	if(strcmp(argv[1],"-c") !=0){
		printf("The command line must follow the following format:\ncat input.file | ./sorter -c  movie_title -d thisdir -o thatdir or \n./sorter -c  movie_title");
	}

	column_to_sort = argv[2];

	if(argc == 3){
		FILE *csv_in = fopen("./movie_metadata.csv","r");
    	char * output = "./sorted_movie_metadata.csv";
    	sortnew(csv_in, output, column_to_sort);
    	fclose(csv_in);   
    	return 0;

	} else if (argc == 5){
		if (strcmp(argv[3],"-d") != 0) {
			  printf("The command line must follow the following format:\n./sorter -c  movie_title -d thisdir -o thatdir");
		} else {
			//time to check if this is a csv file or a directory
			travdir(childPids, argv[4], column_to_sort, NULL);
		}
	} else if (argc == 7){
		if (strcmp(argv[3],"-d") != 0) {
			  printf("The command line must follow the following format:\n./sorter -c  movie_title -d thisdir -o thatdir");
		} else {
			//time to check if this is a csv file or a directory
			//-o output csv files to a certain directory thatdir if they specify -> argv[6] is output directory
			travdir(childPids, argv[4], column_to_sort, argv[6]);
		}
	}

	return 0;
}

//traverse the directory for a csv 
int travdir (pid_t* childPids, const char * dir_name, char* column_to_sort, const char * output_dir)
{
	printf("opening first directory : %s\n",dir_name);
	//counter counts how many child processes
	int counter = 0;
	DIR * directory = opendir(dir_name);

	if (directory == NULL) {
        printf ("Not a valid directory'%s'\n", dir_name);
        return 1;
    }

	//while we go through the directory -> in the parent process keep looking for csv files
	while(directory != NULL)
	{
		struct dirent * currEntry;
		char * d_name;
		currEntry = readdir(directory);
		printf("it got here\n");

		//making sure not to fork bomb
		if(counter==10){
			break;
		}

		if(!currEntry)
		{
			//end of file stream, break-> now wait for children and children's children
			break;
		}
		//d_name is the current directory/file
		d_name = currEntry->d_name;

		//this is a directory 
		if(currEntry->d_type==DT_DIR)
		{
			if(strcmp(d_name,".") != 0 && strcmp(d_name, "..") != 0)
			{
				counter++;
				//need to EXTEND THE PATH for next travdir call, working dir doesn't change (think adir/ -> adir/bdir/....)
				int pathlength = 0;	
				char path[256];
				pathlength = snprintf(path, 256, "%s/%s",currEntry, d_name);
				if(pathlength > 255)
				{
					printf("Path length is too long error");
					return -4;
				}
				//open new directory again
				pid_t pid,ppid;
				//fork returns 0 if child process
				pid = fork();
				//ppid = getppid();

				//if(counter ==0){
					//printf("%sthe initial parent process ", ppid);
				//}

				if (pid < 0) {
          			printf("Failed to fork process 1\n");
         			break;
     			}
				else if(pid==0){ //if child then go to newpath
					//record new pathname
					printf("This is the child process searching through next directory. My pid is %d and my parent's id is %d.\n", getpid(), getppid());
					char * temp2 = malloc(sizeof(char)*256);
					strcpy(temp2,dir_name);
					strcat(temp2,"/");
					strcat(temp2,d_name);
					directory = opendir(temp2);
					free(temp2);
				}
				else if(pid>0){ //parent
					printf("This is the parent process. My pid is %d and my parent's id is %d.\n", getpid(), pid);
                    pid_t child_id;
                    child_id = getpid();
                    //store child process
                    childPids[counter]=child_id;
                    printf( "%sparent forked new child \n", child_id);
				}
				//printf("%s\n",d_name); //error checking and DEBUGGING
			}
		} else if(currEntry->d_type==DT_REG){ 	//regular files, need to check to ensure ".csv"
			//need a for loop to go through the directory to get all csvs - pointer and travdir once at the end of the list of csvs in that one dir	
			printf("checked that the csv is a file\n");
			char pathname [256];
			FILE* csvFile;
			sprintf(pathname, "%s/%s", dir_name, d_name);
			//csvFile = fopen(pathname, "r");

			if (csvFile!=NULL) //check for valid csv file 
			{
				counter++;
				//specify to what directory openddir and then do d_name
				printf("file not null\n");

				pid_t ppid,pid;
				//fork returns 0 if child process
				pid = fork();

				if (pid < 0) {
          			printf("Failed to fork process 1\n");
         			break;
     			}
				//copies the code that you are running and returns zero to a new pid 
				else if(pid==0){ //if child then sort
					printf("This is the child process. My pid is %d and my parent's id is %d.\n", getpid(), getppid());
					//change path for accessing the csv file
					char * temp3 = malloc(sizeof(char)*256);
					strcpy(temp3,dir_name);
					strcat(temp3,"/");
					strcat(temp3,d_name);

					printf("pathname%s\n", temp3);
					printf("directory running in while loop%s\n", directory);
					printf("d_name%s\n",d_name);
					char * path;

					//input to sort: pathname d_name and file pointer csvFile
					//sort returns path of csv it has written to in path d_name
					//temp3 is path to output - right now it is just the directory the csv file is in 
					//path = sort(csvFile, temp3, column_to_sort);
					sortnew(csvFile, temp3, column_to_sort);
					printf("path that child spit out sort process to%s\n", path);
					//have to somehow end the child process when it ends before parent is done
					free(temp3);
				}
				else if(pid>0) { //parent
					printf("This is the parent process. My pid is %d and my parent's id is %d.\n", getpid(), pid);
					pid_t child_id;
					child_id = pid; 
					//store child process
					childPids[counter]=child_id;
				}
			//wait after parent is done looking for csvs and THEN wait for children to end
			//once the child is done fork will return 0 
			} else {
				printf("%s\n", d_name);
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
	printf("closing directory: %s\n", dir_name); //DEBUGGING 
	if(closedir(directory)){
		printf("error could not close dir");
		return -3;
	}

	//outputMetadata(childPids,totalprocesses);
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

