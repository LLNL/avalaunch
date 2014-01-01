#include<stdio.h>

#define BIN_SIZE_OFFSET (1024 * 1024 * 32)

char dummy[BIN_SIZE_OFFSET] = {1};

void main()
{
    printf("This binary is sized %dMB\n",(BIN_SIZE_OFFSET / (1024 * 1024)));

}
