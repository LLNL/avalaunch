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

  spawn_net_channel* ch;
  spawn_net_channel* parent_ch;
  spawn_net_channel_group chgrp;

  spawn_net_chgrp_init(&chgrp, SPAWN_NET_TYPE_TCP);
  printf("Current size of chgrp = %d\n",spawn_net_chgrp_getsize(&chgrp));

  spawn_net_endpoint* ep = spawn_net_open(SPAWN_NET_TYPE_TCP);
  const char* ep_name = spawn_net_name(ep);
  printf("%d: Endpoint name: %s\n", rank, ep_name);

  /* broadcast rank 0 endpoint name to all tasks */
  char parent_name[256];
  strcpy(parent_name, ep_name);
  int len = strlen(ep_name) + 1;
  MPI_Bcast(&len, 1, MPI_INT, 0, MPI_COMM_WORLD);
  MPI_Bcast(parent_name, len, MPI_CHAR, 0, MPI_COMM_WORLD);

  int i;
  if (rank == 0) {
    ch = spawn_net_accept(ep);
    spawn_net_chgrp_add(&chgrp, ch);

    printf("Current size of chgrp = %d\n",spawn_net_chgrp_getsize(&chgrp));

    char str[100];
    int str_len;

    spawn_net_read(ch, &str_len, sizeof(int));
    spawn_net_read(ch, str, (size_t)str_len);
    printf("%d: recevied %s\n", rank, str);
    printf("%d: recevied ch:%s\n", rank, ch->name);
  } else if (rank == 1) {
    ch = spawn_net_connect(parent_name);

    parent_ch = ch;

    char str[] = "hello";
    int str_len = strlen(str) + 1;
    spawn_net_write(ch, &str_len, sizeof(int));
    spawn_net_write(ch, str, (size_t)str_len);
    printf("%d: sent %s\n", rank, str);
    printf("%d: sent ch:%s\n", rank, ch->name);
  }

  if (!rank) {
    char str2[] = "On the road to 6-0!";
    int str2_len = strlen(str2) + 1;
    spawn_net_mcast(&str2_len, sizeof(int), parent_ch, &chgrp, 1);
    spawn_net_mcast(str2, str2_len, parent_ch, &chgrp, 1);
    printf("Root mcasted %s\n",str2);
  } else {
    char str2[100];
    int str2_len;
    spawn_net_mcast(&str2_len, sizeof(int), parent_ch, &chgrp, 0);
    spawn_net_mcast(str2, str2_len, parent_ch, &chgrp, 0);
    printf("Received mcast: %s (size = %d)\n",str2, str2_len);
  }


  spawn_net_disconnect(&ch);
  spawn_net_close(&ep);

  MPI_Finalize();
  return 0;
}
