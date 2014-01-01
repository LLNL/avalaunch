#include<stdio.h>

#define BIN_SIZE_OFFSET (1024 * 1024 * 256)

char dummy[BIN_SIZE_OFFSET] = {1};

int main()
{
    printf("This binary is sized %dMB\n",(BIN_SIZE_OFFSET / (1024 * 1024)));

    return 0;
}
