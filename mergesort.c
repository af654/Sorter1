#include <stdio.h>
#include <string.h>
#include <stdlib.h>

void mergeSort(Row ** rows, int l, int r, int numColumns);
void merge(Row ** rows, int l, int m, int r, int numColumns);
char **colType;
int *colIdx;
int numColumns2;
long doCompare(Row *row1, Row *row2, int index, int count);

int doSort(Row ** rows, int *sortColIdx, char **sortColType, int arrSize,int numColumns){
    numColumns2 = numColumns;
    printf("number of columns to sort: %d\n", numColumns);
    colIdx = sortColIdx;
    colType = sortColType;
    printf("colIdx making sure the correct indeces: %d %d\n", colIdx[0], colIdx[1]);
    //printf("column type is correct or nah: %s %s \n", *(colType),*(colType + 1)); 
    printf("is this how we access the column types? %s %s \n", colType[0], colType[1]);
    mergeSort(rows, 0, arrSize-1, numColumns2);

    return 0;
}

void mergeSort(Row **rows, int l, int r, int numColumns2) {
    printf("it got into mergesort");
    if (l < r){
        
        int m=(l+(r-1))/2;
        
        mergeSort(rows, l, m, numColumns2);
        
        mergeSort(rows, m+1, r, numColumns2);
        
        merge(rows, l, m, r, numColumns2);
    }
    
}

void merge(Row **row, int left, int mid, int right, int numColumns2) {
	int i,j,k,c,count,index;
    int n1 = mid - left + 1;
    int n2 =  right - mid;
    
    Row *L[n1], *R[n2];
    
    
    for (i = 0; i < n1; i++) {
		L[i] = row[left + i];	
	}
    for (j = 0; j < n2; j++) {
		R[j] = row[mid + 1+ j];
	}
        
	i = 0;
	j = 0;
	k = left;
    while (i < n1 && j < n2) {
        printf("in the while loopy\n");
        int c;
        count = 0;
        //For extra credit 1 the code would hav e to be here
        //There would be three cases (<,>, and =). The < and > are alrady good
        //If the are equal then we need to step through to the value to sort by in the cascade
        //The docompare will now take an integer value to mean which index of the input array to get the column from
        index = colIdx[count];
        printf("column index first one: %d\n", index);
        printf("count tells us which column we are working with the first or second %d \n",count);
        c = doCompare(L[i], R[j], index,count);
        printf("the return value of doCompare: %d\n",c);
        //count counts the number of indeces we have gone through
        count++;

        if(c == 0 && count < numColumns2) {
            printf("number of columns to sort: %d\n", numColumns2);
            while(c == 0 && count < numColumns2) {
                index = colIdx[count];
                printf("column index second one: %d\n", index);
                c = doCompare(L[i], R[j], index,count);
                count++;
                if (c < 0) {
                    row[k] = L[i];
                    i++;
                } else if (c > 0){
                    row[k] = R[j];
                    j++;
                }
           }
        } else if (c < 0) {
            row[k] = L[i];
            i++;
        } else {
            row[k] = R[j];
            j++;
        }
        k++;
    }
    
	while (i < n1) {
        row[k] = L[i];
        i++;
        k++;
    }
    
    
    while (j < n2) {
        row[k] = R[j];
        j++;
        k++;
    }
    
}


long doCompare(Row *row1, Row *row2, int index, int count) {
    int counter = count;
    printf("Column index will tell us what column we are sorting: %d\n", index);
    const char* r1Value = (row1->colEntries)[index].value;
    const char* r2Value = (row2->colEntries)[index].value;
    printf("but what are the values they are comparing %s %s \n", r1Value, r2Value);
    if (strcmp(colType[counter], "char") == 0) {
        //Skip the quotes
        printf("compared the values to a char");
        if (*r1Value == '"') {
            r1Value++;
        }
        if (*r2Value == '"') {
            r2Value++;
        }
        return(strcmp(r1Value, r2Value));
    } else if (strcmp(colType[counter], "int") == 0){
                printf("it is comparing integers in the doCompare");
                int i1Value = atoi(r1Value);
                int i2Value = atoi(r2Value);
                return (i1Value - i2Value);
    } else if (strcmp(colType[counter], "long") == 0) {
                long l1Value = atol(r1Value);
                long l2Value = atol(r2Value);
                return (l1Value - l2Value);
    } else if (strcmp(colType[counter], "float") == 0) {
                float f1Value = atof(r1Value);
                float f2Value = atof(r2Value);
                return (f1Value - f2Value);
    } else {
        fprintf(stderr, "Unknown colType: %s\n", colType[counter]);
        exit(1);
    }
}
