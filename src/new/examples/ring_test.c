#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>

#include <sys/types.h>
#include <unistd.h>

#include "ring.h"

int main(int argc, char* argv[])
{
  /* create address (e.g., open socket and encode as string) */
  char addr[256];
  pid_t pid = getpid();
  snprintf(addr, sizeof(addr), "%llu", (unsigned long long)pid);

  /* get addresses of left and right procs */
  uint64_t rank, ranks;
  char *left, *right;
  int rc = ring_create(addr, &rank, &ranks, &left, &right);

  /* print what we got */
#if 0
  printf("Rank %d, Size %d, Left %s, Me %s, Right %s\n",
    rank, ranks, left, addr, right
  );
  fflush(stdout);
#endif

  /* free left and right strings allocated in ring_create */
  free(left);
  free(right);

  return 0;
}
