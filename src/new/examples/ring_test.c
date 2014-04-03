#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>

#include <sys/types.h>
#include <unistd.h>

#include "ring.h"
#include "strmap.h"
#include "spawn_net.h"
#include "lwgrp.h"

int main(int argc, char* argv[])
{
  /* create address (e.g., open socket and encode as string) */
  //spawn_net_endpoint* ep = spawn_net_open(SPAWN_NET_TYPE_TCP);
  spawn_net_endpoint* ep = spawn_net_open(SPAWN_NET_TYPE_IBUD);
  const char* ep_name = spawn_net_name(ep);

  /* get addresses of left and right procs */
  uint64_t rank, ranks;
  char *left, *right;
  //int rc = ring_create(ep_name, &rank, &ranks, &left, &right);
  int rc = ring_create2(ep_name, &rank, &ranks, &left, &right);

#if 0
  /* print what we got */
  printf("Rank %d, Size %d, Left %s, Me %s, Right %s\n",
    rank, ranks, left, ep_name, right
  );
  fflush(stdout);
#endif

#if 1
  /* create to log(N) tasks on left and right sides */
  lwgrp* group = lwgrp_create(ranks, rank, ep_name, left, right, ep);

  /* free left and right strings allocated in ring_create */
  spawn_free(&left);
  spawn_free(&right);

  char split_str[256];
  //sprintf(split_str, "%d", (rank/2)*100);
  sprintf(split_str, "hello");
  lwgrp* newgroup = lwgrp_split_str(group, split_str);
  lwgrp_barrier(newgroup);
  lwgrp_free(&newgroup);

  /* gather endpoint addresses for all procs */
  strmap* map = strmap_new();
  strmap_setf(map, "%d=%s", rank, ep_name);

  lwgrp_allgather_strmap(map, group);
#endif

#if 0
  if (rank == 0) {
    int64_t i;
    for (i = 0 ; i < ranks; i++) {
      char* value = strmap_getf(map, "%d", i);
      printf("%d --> %s\n", i, value);
    }
    fflush(stdout);
  }
#endif

#if 1
  strmap_delete(&map);

  /* disconnect and free group */
  lwgrp_free(&group);
#endif

  spawn_net_close(&ep);

  return 0;
}
