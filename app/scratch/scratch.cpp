#include <stdio.h>

int main()
{
    int* columns;
    columns = new int[3];
    columns[0] = 0; columns[1] = 1; columns[2] = 2;
    printf("%d", columns[2]);
    delete [] columns;
}
