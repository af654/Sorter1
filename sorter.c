#include <stdio.h>
#include <string.h>
#include "sorter.h"
#include "mergesort.c"

#define NUM_COLS 28 //Code works for 28 columns
#define NUM_ROWS 8192 //Max number of rows in a file is 8192
#define MAX_ENTRY_SIZE 256
#define MAX_TYPE_SIZE 7

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

void sortnew(FILE* csv_in, FILE* csv_out, char * columnToSort) {
    int i,j;
    int columnToSortIndex, validNumRows;
    int rowIndex = 0;
    int *columnIndeces;
    int *ptr;
    char **columnToSortTypes;
    char* columnToSortType;
    char* line = NULL;
    char* token, *tokenType, *headerLine;            
    size_t size = 128;    
    //Allocate space for number of rows in file
    Row *rows[NUM_ROWS];
    printf("starting to sort the file now\n");

    //Get index of the column we want to sort by
    //columnToSortIndex = isValidColumn(columnToSort);
    //should be 2
    columnIndeces = (int *) malloc(NUM_COLS * sizeof(int));
    getColumnIndeces(columnToSort,columnIndeces);
    printf("ARE THESE CORRECT NOW : %d %d \n",columnIndeces[0],columnIndeces[1]);

    /*if(columnToSortIndex == -1) {
        printf("ERROR COLUMN IS NOT VALID!\n");
        exit(1);
    }*/
    //columnToSortType = validColumnTypes[columnToSortIndex];
    //Get an array of strings to have the types

    columnToSortTypes = (char **)malloc(sizeof(char *) * 10);
    for (i = 0; i < 2; i++) {
        columnToSortTypes[i] = (char *)malloc(sizeof(char) * 10);
        //*(columnToSortTypes + i) = (char *)malloc(MAX_TYPE_SIZE);
    }

    setColumnTypes(columnIndeces, columnToSortTypes);
    printf("the types should be correct now");
    printf("columnToSortTypes are this after calling set column types %s %s\n", columnToSortTypes[0],columnToSortTypes[1]);

    if(NULL == csv_in){
        printf("NULL FILE");
    }
    //Skip first row
    char * input;
    line = (char *)malloc(1024 * sizeof(char));
    getline(&line, &size, csv_in);
    
    headerLine = (char *) malloc(strlen(line));
    strcpy(headerLine,line);
    if(NULL == csv_in){
        printf("NULL FILE");
    }

    int c;
    printf("parsing the csv file\n");    
    while(!feof(csv_in)) {
        int colIndex = 0;
        int i;

        line = NULL;
        getline(&line, &size, csv_in);  

        //Begining of a new row starts here
        rows[rowIndex] = malloc(sizeof(Row));
        
        //Gets first column
        token = strtok_single(line, ",\n");
        if(!token)
            break;
        tokenType = findType(token);

        //Set the values in the rows matrix
        rows[rowIndex]->colEntries[colIndex].value = (char *) malloc(strlen(token) + 1);
        strcpy(rows[rowIndex]->colEntries[colIndex].value, token);
        rows[rowIndex]->colEntries[colIndex].type = (char *) malloc(strlen(tokenType) + 1);
        strcpy(rows[rowIndex]->colEntries[colIndex].type, tokenType);
        colIndex++;
                
        while(token) {
            //Tokenizes the string based on ','
            //Starts from first column onward until end
            //If the first character in the line is a " then we tokenize based on the quotation mark
            token = strtok_single(NULL, ",\r\n");
            if(!token)
                break;
            
            tokenType = findType(token);
            //Set the values in the rows matrix
            rows[rowIndex]->colEntries[colIndex].value = (char *) malloc(strlen(token) + 1);
            strcpy(rows[rowIndex]->colEntries[colIndex].value, token);            
            rows[rowIndex]->colEntries[colIndex].type = (char *) malloc(strlen(tokenType) + 1);
            strcpy(rows[rowIndex]->colEntries[colIndex].type, tokenType);

            colIndex++;
        }
        rowIndex++;
    }
    validNumRows = rowIndex;

    printf("Values have been allocated in rows\n");    
    //Implment the sorting and call here
    //change columnToSortIndex to an array of integrs
    //doSort(rows,columnToSortIndex,columnToSortType,validNumRows);
    doSort(rows,columnIndeces,columnToSortTypes,validNumRows, 2);

    printf("the file should be sorted\n");

    //Print to a CSV file
    fprintf(csv_out, headerLine);
    printToCSV(csv_out, rows, validNumRows, NUM_COLS);

    fclose(csv_out);
    fclose(csv_in);
        
    return;
 //end of function
}

void printToCSV(FILE *csv_out, Row ** rows, int validNumRows, int validNumCols) {
    int i,j;
    //Loop through the rows
    for (i = 0; i < validNumRows; i++) {
        //Loop through the columns
        for(j = 0; j < validNumCols-1; j++) {
            fprintf(csv_out, "%s,", rows[i]->colEntries[j].value);
        }
        fprintf(csv_out, "%s\n", rows[i]->colEntries[j].value);        
    } 
}

void getColumnIndeces(char* columnNames, int* columnIndeces) {
    //Instead of treating as a single string, treat as comma separated vcalues, tokenize baseed on that
    //Gets first column
    char* token;
    printf("columnNames: %s\n", columnNames);

    int validColumnCounter = 0;
    char *columnNamesCopy = (char *) malloc(strlen(columnNames) + 1);
    strcpy(columnNamesCopy, columnNames);

    if(strrchr(columnNamesCopy, ',') != strlen(columnNamesCopy) - 1) {
        strcat(columnNamesCopy, ",");
    }

    token = strtok_single(columnNamesCopy, ",");
    if(!token) {
        printf("Column input is not valid.\n");
        return -1;
    }

    printf("column that should be separated is: %s\n",token);
    //prints director_facebook_likes
    int tokenColumn = isValidColumn(token);
    printf("tokenColumn should return the indeces 5 or 10 for example %d\n",tokenColumn);
    if(tokenColumn == -1) { //default to director_name
        printf("Not a valid column, defaulting to director_name");
        columnIndeces[validColumnCounter] = 1; 
    } else {
        columnIndeces[validColumnCounter] = tokenColumn; 
    }
    validColumnCounter++;
    printf("counter of valid columns%d\n",validColumnCounter);

    while(token) {
        token = strtok_single(NULL, ",");
        if(!token)
            break;
        //prints movie_title
        printf("column that should be separated is: %s\n",token);
        int tokenColumn = isValidColumn(token);
        printf("tokenColumn should return the indeces 5 or 10 for example %d\n",tokenColumn);

        if(tokenColumn == -1) { //default to director_name
            printf("Not a valid column, defaulting to director_name");
            columnIndeces[validColumnCounter] = 2; 
        } else {
            columnIndeces[validColumnCounter] = tokenColumn; 
        }
        validColumnCounter++;
        printf("counter of valid columns%d\n",validColumnCounter);
    }

    columnIndeces = realloc(columnIndeces, validColumnCounter*sizeof(int));
    printf("counter of valid columns RIGHT BEFORE RETURNING THE FUNCTION %d\n",validColumnCounter);
}

//Returns an array for strings that contain the types of the columns that will be sorted by
void setColumnTypes(int *columnIndeces, char **columnToSortTypes) {
    int i;
    //shouldnt be 2 change it to how many columns there are - maybe validColumnCounter??
    for(i = 0; i < 2; i++) {
        printf("findtype function returns %s \n", findType(validColumnTypes[columnIndeces[i]]));
        strcpy(columnToSortTypes[i],findType(validColumnTypes[columnIndeces[i]]));
        printf("columnToSortTypes in the function setColumnTypes %s\n", columnToSortTypes[i]);
    }
}

//Returns index of valid column, if not valid return -1
int isValidColumn(char* columnName) {
    int i;
    for(i = 0; i < (sizeof(validColumns) / sizeof(validColumns[i])); i++){
        if(strcmp(validColumns[i], columnName) == 0) {
            return i;
        }
    }
    return -1;
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


//Tokenize a given input based on a delimiter, returns NULL if consecutive delimiters
char* strtok_single (char * string, char const * delimiter) {
    static char *source = NULL;
    char *p, *result = 0;
    if(string != NULL)         source = string;
    if(source == NULL)         return NULL;
 
    if((p = strpbrk (source, delimiter)) != NULL) {
        char *s = p+1;
        if(*(s) == '"') { //Enountered a movie title that has commas in it
            while(*(s+1) != '"') { //Go through util the next " and replace the commas with ;
                if(*(s) == ',') {
                    *s = ';';
                }
                s++;
            }
        }

       *p = 0;
       result = source;
       source = ++p;
    }
 return result;
}