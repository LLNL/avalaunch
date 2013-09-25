#include <stdio.h>
#include <stdlib.h>
#include "strmap.h"

int main(int argc, char* argv[])
{
  strmap* map = strmap_new();
  strmap_set(map, "hi", "there");
  strmap_set(map, "hello", "world");
  strmap_set(map, "go", "bucks!");
  strmap_set(map, "beat2", "cal");
  strmap_set(map, "beat10", "cal");
  strmap_set(map, "beat0", "cal");
  strmap_set(map, "beat1", "cal");

  strmap_setf(map, "newstr=value");
  strmap_setf(map, "newstr1=%d", 1);
  strmap_setf(map, "newstr2=%f", 2.5);
  strmap_setf(map, "newstr3-%d=%f", 1, 2.5);

  void* buf = NULL;
  size_t bufsize = strmap_pack_size(map);
  if (bufsize > 0) {
    buf = malloc(bufsize);
    strmap_pack(buf, map);
  }

  strmap* map2 = strmap_new();
  strmap_unpack(buf, map2);

  if (buf != NULL) {
    free(buf);
  }

  strmap_node* node;
  for (node = strmap_node_first(map2);
       node != NULL;
       node = strmap_node_next(node))
  {
    const char* key = strmap_node_key(node);
    const char* val = strmap_node_value(node);
//    printf("%s --> %s\n", key, val);
  }

  const char* value;
  value = strmap_get(map2, "go");
  printf("go --> %s\n", value);
  value = strmap_getf(map2, "beat%d", 10);
  printf("beat10 --> %s\n", value);
  printf("\n");

  strmap_set(map2, "go", "OH-IO!");
  strmap_setf(map2, "beat%d=Cal", 10);
  value = strmap_get(map2, "go");
  printf("go --> %s\n", value);
  value = strmap_getf(map2, "beat%d", 10);
  printf("beat10 --> %s\n", value);
  printf("\n");

  strmap_print(map2);

  strmap_unset(map2, "go");
  strmap_unsetf(map2, "beat%d", 2);
  strmap_print(map2);

  strmap_delete(&map2);
  strmap_delete(&map);
  return 0;
}
