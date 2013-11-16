#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "mpi.h"

#include "spawn_internal.h"

int main(int argc, char* argv[])
{
  MPI_Init(&argc, &argv);

  int rank, ranks, iters = 1000;
  MPI_Comm_rank(MPI_COMM_WORLD, &rank);
  MPI_Comm_size(MPI_COMM_WORLD, &ranks);

  spawn_net_endpoint* ep = spawn_net_open(SPAWN_NET_TYPE_IBUD);
  const char* ep_name = spawn_net_name(ep);
  printf("%d: Endpoint name: %s\n", rank, ep_name);

  /* broadcast rank 0 endpoint name to all tasks */
  char parent_name[256];
  strcpy(parent_name, ep_name);
  int len = strlen(ep_name) + 1;
  MPI_Bcast(&len, 1, MPI_INT, 0, MPI_COMM_WORLD);
  MPI_Bcast(parent_name, len, MPI_CHAR, 0, MPI_COMM_WORLD);

  spawn_net_channel** chs = (spawn_net_channel**) malloc(ranks * sizeof(spawn_net_channel));

  int i;
  int j;
  int size = 1;
  char str[1<<20];
  memset(str, 1, sizeof(str));
  for (size = 1; size < 1<<19; size *= 2) {
    if (size > 2048) {
      iters = 100;
    } else {
      iters = 1000;
    }
    if (rank == 0) {
      for (i = 1; i < ranks; i++) {
        chs[i] = spawn_net_accept(ep);
      }
  
      for (j = 0; j < iters; ++j) {
        for (i = 1; i < ranks; i++) {
          int str_len;
          spawn_net_read(chs[i], &str_len, sizeof(int));
          spawn_net_read(chs[i], str, (size_t)str_len);
          //printf("%d: recevied %s\n", rank, str);
          //printf("%d: recevied ch:%s\n", rank, chs[i]->name);
        }
      }
      for (i = 1; i < ranks; i++) {
        printf("%d: recevied ch:%s; %d Bytes %d times\n", rank, chs[i]->name, size, iters);
      }
  
      for (i = 1; i < ranks; i++) {
        spawn_net_disconnect(&chs[i]);
      }
    } else {
      spawn_net_channel* ch = spawn_net_connect(parent_name);
  
      for (j = 0; j < iters; ++j) {
        spawn_net_write(ch, &size, sizeof(int));
        spawn_net_write(ch, str, (size_t)size);
      }
      //printf("%d: sent %s\n", rank, str);
      printf("%d: sent ch:%s; %d bytes %d times\n", rank, ch->name, size, iters);
  
      spawn_net_disconnect(&ch);
    }
  }

  free(chs);

  spawn_net_close(&ep);

  MPI_Finalize();
  return 0;
}
