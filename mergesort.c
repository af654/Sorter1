#include <stdio.h>
#include <string.h>
#include <stdlib.h>

void mergeSort(Row ** rows, int l, int r, int numColumns);
void merge(Row ** rows, int l, int m, int r, int numColumns);
char **colType;
int *colIdx;
long doCompare(Row *row1, Row *row2, int index);

int doSort(Row ** rows, int *sortColIdx, char **sortColType, int arrSize,int numColumns){
    colIdx = sortColIdx;
    colType = sortColType;
    
    mergeSort(rows, 0, arrSize-1, numColumns);
    
    return 0;
}

void mergeSort(Row **rows, int l, int r, int numColumns) {
    if (l < r){
        
        int m=(l+(r-1))/2;
        
        mergeSort(rows, l, m, numColumns);
        
        mergeSort(rows, m+1, r, numColumns);
        
        merge(rows, l, m, r, numColumns);
    }
    
}

void merge(Row **row, int left, int mid, int right, int numColumns) {
	int i,j,k,c,count;
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
        int c;
        //For extra credit 1 the code would hav e to be here
        //There would be three cases (<,>, and =). The < and > are alrady good
        //If the are equal then we need to step through to the value to sort by in the cascade
        //The docompare will now take an integer value to mean which index of the input array to get the column from
        count = 0;
        c = doCompare(L[i], R[j], count++);
        if(c == 0 && count < numColumns) {
            while(c == 0 && count < numColumns) {
                c = doCompare(L[i], R[j], count++);                
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


long doCompare(Row *row1, Row *row2, int index) {
    const char* r1Value = (row1->colEntries)[colIdx[index]].value;
    const char* r2Value = (row2->colEntries)[colIdx[index]].value;
    
    if (strcmp(colType[index], "char") == 0) {
		//Skip the quotes
        if (*r1Value == '"') {
            r1Value++;
        }
        if (*r2Value == '"') {
            r2Value++;
        }
        return(strcmp(r1Value, r2Value));
    } else if (strcmp(colType[index], "int") == 0){
                int i1Value = atoi(r1Value);
                int i2Value = atoi(r2Value);
                return (i1Value - i2Value);
    } else if (strcmp(colType[index], "long") == 0) {
                long l1Value = atol(r1Value);
                long l2Value = atol(r2Value);
                return (l1Value - l2Value);
    } else if (strcmp(colType[index], "float") == 0) {
                float f1Value = atof(r1Value);
                float f2Value = atof(r2Value);
                return (f1Value - f2Value);
    } else {
        fprintf(stderr, "Unknown colType: %s\n", colType);
        exit(1);
    }
}
