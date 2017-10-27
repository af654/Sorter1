typedef struct rowEntry {
    char* value;
    char* type;
} rowEntry;

typedef struct Row {
    struct rowEntry *colEntries;
} Row;

size_t parseline(char** lineptr, size_t *n, FILE *stream);
char* strtok_single(char* str, char const* delims);
char* findType(char* token);
int isValidColumn(char* columnName);
void printToCSV(FILE *csv_out, Row ** rows, int numRows, int numCols);
//int travdir (pid_t* childPids, const char * dir_name, char* column_to_sort, const char * output_dir);
//int outputMetadata(pid_t* childPids, int totalprocesses);
char * sortnew(FILE *fp, char * pathToOutput, char * columnToSort);