#include <stdio.h>
#include <string.h>
#include "sorter.h"
#include "mergesort.c"

#define NUM_COLS 28 //Code works for 28 columns
#define NUM_ROWS 8192 //Max number of rows in a file is 8192
#define MAX_ENTRY_SIZE 256

//int main(int argc, char* argv[]) {
    //FILE *csv_in = fopen("./movie_metadata.csv","r");
    
    //char * output = "./sorted_movie_metadata.csv";
    //sort(csv_in, output, "num_critic_for_reviews");

  //  fclose(csv_in);    
//}

const char* validColumns[] = {
    "color",
    "director_name",
    "num_critic_for_reviews",
    "duration",
    "director_facebook_likes",
    "actor_3_facebook_likes",
    "actor_2_name",
    "actor_1_facebook_likes",
    "gross",
    "genres",
    "actor_1_name",
    "movie_title",
    "num_voted_users",
    "cast_total_facebook_likes",
    "actor_3_name",
    "facenumber_in_poster",
    "plot_keywords",
    "movie_imdb_link",
    "num_user_for_reviews",
    "language",
    "country",
    "content_rating",
    "budget",
    "title_year",
    "actor_2_facebook_likes",
    "imdb_score",
    "aspect_ratio",
    "movie_facebook_likes"
};

const char* validColumnTypes[] = {
    "char",
    "char",
    "int",
    "int",
    "int",
    "int",
    "char",
    "int",
    "int",
    "char",
    "char",
    "char",
    "int",
    "int",
    "char",
    "int",
    "char",
    "char",
    "int",
    "char",
    "char",
    "char",
    "int",
    "int",
    "int",
    "float",
    "float",
    "int"
};

char * sortnew(FILE* csv_in, char * pathToCSV, char * columnToSort) {
    printf("testing sort\n");
    int i,j;
    int columnToSortIndex, validNumRows;
    int rowIndex = 0;
    char* columnToSortType;
    char* line = NULL;
    char* token, *tokenType, *headerLine;            
    size_t size;    
    FILE* csv_out; 
    //Allocate space for number of rows in file
    Row *rows[NUM_ROWS];
    printf("starting to sort the file now\n");

    //Get index of the column we want to sort by
    columnToSortIndex = isValidColumn(columnToSort);
    if(!columnToSortType) {
        printf("ERROR COLUMN IS NOT VALID!\n");
        return(1);
    }
    columnToSortType = validColumnTypes[columnToSortIndex];
    csv_in = fopen(pathToCSV,"r");

    printf("file output path:%s\n", pathToCSV);
    printf("sorting this column: %s\n", columnToSort);
    
    //Skip first row
    char * input;
    input = getline(&line, &size, csv_in);
    printf("%s\n",input);
    headerLine = (char *) malloc(strlen(line));
    strcpy(headerLine,line);
    if(NULL == csv_in){
        printf("NULL FILE");
    }

    int c;
    printf("input%s\n", input);
    while(!feof(csv_in)) {
        printf("parsing the csv file");
        int colIndex = 0;
        int i;

        //if(feof(csv_in)){
          //break;
        //}
        //Get the next line from the input
        line = NULL;
        getline(&line, &size, csv_in);  

        //Begining of a new row starts here
        rows[rowIndex] = (Row *) malloc(sizeof(Row *));
        for(i = 0; i < NUM_COLS; i++) {
            rows[rowIndex]->colEntries = (char *) malloc(MAX_ENTRY_SIZE);
        }

        //Gets first column
        token = strtok_single(line, ",\n");
        if(!token)
            break;
        tokenType = findType(token);

        //printf("%s\t\t\t %s\n", token, tokenType);
        //Set the values in the rows matrix
        rows[rowIndex]->colEntries[colIndex].value = (char *) malloc(strlen(token) + 1);
        strcpy(rows[rowIndex]->colEntries[colIndex].value, token);
        rows[rowIndex]->colEntries[colIndex].type = (char *) malloc(strlen(tokenType) + 1);
        strcpy(rows[rowIndex]->colEntries[colIndex].type, tokenType);
        colIndex++;
        
        while(token) {
            //Tokenizes the string based on ','
            //Starts from first column onward until end
            token = strtok_single(NULL, ",\n");
            if(!token)
                break;
            
            tokenType = findType(token);
            //Set the values in the rows matrix
            rows[rowIndex]->colEntries[colIndex].value = (char *) malloc(strlen(token) + 1);
            strcpy(rows[rowIndex]->colEntries[colIndex].value, token);            
            rows[rowIndex]->colEntries[colIndex].type = (char *) malloc(strlen(tokenType) + 1);
            strcpy(rows[rowIndex]->colEntries[colIndex].type, tokenType);
            
            
            /*
            if(strcmp(tokenType,"int") == 0) { //Input is an integer

            } else if (strcmp(tokenType,"char") == 0) { //Input is a string
                
            } else if (strcmp(tokenType,"float") == 0) { //Input is a float

            } else { //Input is not a valid type

            }*/

            //printf("%s\t\t\t %s\n", token, tokenType);
            colIndex++;
        }
        //printf("\n");
        rowIndex++;       
    }
    validNumRows = rowIndex;

    //Implment the sorting and call here
    doSort(rows,columnToSortIndex,columnToSortType,validNumRows);

    printf("the file should be sorted\n");

    //Print to a CSV file
    csv_out = fopen(pathToCSV,"w");
    fprintf(csv_out, headerLine);
    printToCSV(csv_out, rows, validNumRows, NUM_COLS);

    fclose(csv_out);
    fclose(csv_in);
    //Free Memory
    for(i = 0; i < validNumRows; i++) {
        for(j = 0; j < NUM_COLS; j++) {
            free(rows[i]->colEntries[j].value);
            free(rows[i]->colEntries[j].type);
        }
        free(rows[i]);
    }
        
    return pathToCSV;
 //end of function
}

void printToCSV(FILE *csv_out, Row ** rows, int validNumRows, int validNumCols) {
    int i,j;
    //Loop through the rows
    for (i = 0; i < validNumRows; i++) {
        //Loop through the columns
        for(j = 0; j < validNumCols; j++) {
            fprintf(csv_out, "%s,", rows[i]->colEntries[j].value);
        }
        fprintf(csv_out, "\n", rows[i]->colEntries[j].value);
    } 
}

//Returns index of valid column, if not valid return 0
int isValidColumn(char* columnName) {
    int i;
    for(i = 0; i < (sizeof(validColumns) / sizeof(validColumns[i])); i++){
        if(strcmp(validColumns[i], columnName) == 0) {
            return i;
        }
    }
    return 0;
}

//Returns the type of a given string token, does NOT perfom atoi() or atof()
char * findType(char* token) {
    int length,i; 

    length = strlen (token);
    for (i = 0; i < length; i++) {
        if(token[i] != '\r') {
            //If a character is not a digit but it is also not a period it must be a string
            if (!isdigit(token[i]) && token[i] != '.') {
                return "char";
            } else if (!isdigit(token[i])){ //otherwise it is a float
                return "float";
            } 
        }
            
    } //If we never encounter a non-numerical it must be an integer
    return "int";
}

//Get a line from a file stream
size_t parseline(char **lineptr, size_t *n, FILE *stream) {
    char *bufptr = NULL;
    char *p = bufptr;
    size_t size;
    int c;

    if (lineptr == NULL) {
        return -1;
    } 
    if (stream == NULL) {
        return -1;
    }
    if (n == NULL) {
        return -1;
    }
    bufptr = *lineptr;
    size = *n;

    c = fgetc(stream);
    if (c == EOF) {
        return -1;
    }
    if (bufptr == NULL) {
        bufptr = malloc(2048);
        if (bufptr == NULL) {
            return -1;
        }
        size = 4096;
    }
    p = bufptr;
    while(c != EOF) {
        if ((p - bufptr) > (size - 1)) {
            size = size + 128;
            bufptr = realloc(bufptr, size);
            if (bufptr == NULL) {
                return -1;
            }
        }
        *p++ = c;
        if (c == '\n') {
            break;
        }
        c = fgetc(stream);
    }

    *p++ = '\0';
    *lineptr = bufptr;
    *n = size;
    
    return p - bufptr - 1;
}

//Tokenize a given input based on a delimiter, returns NULL if consecutive delimiters
char* strtok_single (char * string, char const * delimiter) {
    static char *source = NULL;
    char *p, *result = 0;
    if(string != NULL)         source = string;
    if(source == NULL)         return NULL;
 
    if((p = strpbrk (source, delimiter)) != NULL) {
       *p  = 0;
       result = source;
       source = ++p;
    }
 return result;
}
