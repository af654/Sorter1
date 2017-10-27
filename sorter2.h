/*****
*
*   Define structures and function prototypes for your sorter
*
*
*
******/
#include <unistd.h>
int travdir (pid_t* childPids, const char * dir_name, char* column_to_sort, const char * output_dir);
int outputMetadata(pid_t* childPids, int totalprocesses);