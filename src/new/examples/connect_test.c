#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "mpi.h"

#include "spawn_internal.h"

int main(int argc, char* argv[])
{
  MPI_Init(&argc, &argv);

  int rank, ranks;
  MPI_Comm_rank(MPI_COMM_WORLD, &rank);
  MPI_Comm_size(MPI_COMM_WORLD, &ranks);

  spawn_endpoint_t ep;
  spawn_channel_t ch;
  spawn_net_open(SPAWN_EP_TYPE_TCP, &ep);
  const char* ep_name = spawn_net_name(&ep);
  printf("%d: Endpoint name: %s\n", rank, ep_name);

  int len = strlen(ep_name) + 1;
  MPI_Bcast(&len, 1, MPI_INT, 0, MPI_COMM_WORLD);
  if (rank == 0) {
    MPI_Bcast((void*)ep_name, len, MPI_CHAR, 0, MPI_COMM_WORLD);
    spawn_net_accept(&ep, &ch);

    char str[100];
    int str_len;
    spawn_net_read(&ch, &str_len, sizeof(int));
    spawn_net_read(&ch, str, (size_t)str_len);
    printf("%d: recevied %s\n", rank, str);
  } else {
    char remote[256];
    MPI_Bcast(remote, len, MPI_CHAR, 0, MPI_COMM_WORLD);
    spawn_net_connect(remote, &ch);

    char str[] = "hello";
    int str_len = strlen(str) + 1;
    spawn_net_write(&ch, &str_len, sizeof(int));
    spawn_net_write(&ch, str, (size_t)str_len);
    printf("%d: sent %s\n", rank, str);
  }

  spawn_net_disconnect(&ch);
  spawn_net_close(&ep);

  MPI_Finalize();
  return 0;
}
