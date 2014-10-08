#include <stdio.h>
#include <stdlib.h>

#include "spawn.h"

/* return number of LIBS in map */
static int lib_num(strmap* map)
{
    int num = 0;
    const char* count = strmap_get(map, "LIBS");
    if (count != NULL) {
        num = atoi(count);
    }
    return num;
}

int main(int argc, char* argv[])
{
  int i;

  if (argc != 2) {
    printf("Usage: readlibs <exe>\n");
    return 1;
  }

  const char* file = argv[1];

  strmap* map = strmap_new();
  int rc = lib_capture(map, file);

  int libs = lib_num(map);
  for (i = 0; i < libs; i++) {
    const char* libname = strmap_getf(map, "LIB%d", i);
    printf("%s\n", libname);
  }

  strmap_delete(&map);

  return rc;
}
