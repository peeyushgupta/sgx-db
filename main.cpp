#include "db.cpp"
#include <cstdio>

int main(){
    u8 i1[15],i2[15];
    *((u64*)i1) = 1;
    *((u64*)i2) = 2;
    i1[8] = 'A';
    i2[8] = 'A';
    i1[9] = 'B';
    i2[9] = 'A';
    printf("%lld\n", varchar_cmp((u8*)i1, (u8*)i2, 5));

}