typedef struct rowEntry {
    const char * value;
    const char * type;
} rowEntry;

typedef struct Row {
    struct rowEntry colEntries[28];
} Row;

void getColumnIndeces(char* columnNames, int * columnIndeces);   
void setColumnTypes(int *columnIndeces, char **columnToSortTypes);
size_t parseline(char** lineptr, size_t *n, FILE *stream);
char* strtok_single(char* str, char const* delims);
char* findType(char* token);
int isValidColumn(char* columnName);
void printToCSV(FILE *csv_out, Row ** rows, int numRows, int numCols);
void sortnew(FILE *fp_in, FILE *fp_out, char * columnToSort);