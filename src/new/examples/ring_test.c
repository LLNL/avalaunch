#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>

#include <sys/types.h>
#include <unistd.h>

#include "ring.h"
#include "strmap.h"
#include "spawn_net.h"
#include "spawn_net_util.h"

#define LWGRP_SUCCESS (0)
#define LWGRP_FAILURE (1)

/* We represent groups of processes using a doubly-linked list called
 * a "chain".  This is a very simple struct that records the number
 * of processes in the group, the rank of the local process within the
 * group, and the address of the local process and of the processes
 * having ranks one less (left) and one more (right) than the local
 * process. */

/* A logchain is a data structure that records the number and addresses
 * of processes that are 2^d ranks away from the local process to the
 * left and right sides, for d = 0 to d = ceiling(log N)-1.
 *
 * When multiple collectives are to be issued on a given chain, one
 * can construct and cache the logchain as an optimization.
 *
 * A logchain can be constructed by executing a collective operation on
 * a chain, or it can be filled in locally given a communicator as the
 * initial group.  It must often be used in conjunction with a chain
 * in communication operations. */

typedef struct lwgrp_chain {
  int64_t size;      /* number of processes in our group */
  int64_t rank;      /* our rank within the group [0,group_size) */
  char* name;        /* strdup of address (rank) of current process */
  char* left;        /* strdup of address (rank) of process one less than current */
  char* right;       /* strdup of address (rank) of process one more than current */
  int64_t list_size; /* number of elements in channel lists */
  spawn_net_channel** list_left;  /* process addresses for 2^d hops to the left */
  spawn_net_channel** list_right; /* process addresses for 2^d hops to the right */
  spawn_net_endpoint* ep; /* pointer to endpoint to accept connections */
} lwgrp_chain;

/* waits for channel in specified round to connect */
static int lwgrp_accept_left(lwgrp_chain* group, int target_round)
{
  /* return immediately if we already accepted this connection */
  if (group->list_left[target_round] != SPAWN_NET_CHANNEL_NULL) {
    return LWGRP_SUCCESS;
  }

  /* otherwise, wait until we accept a connection on the specified round */
  int round = -1;
  spawn_net_endpoint* ep = group->ep;
  while (round != target_round) {
      /* accept a connection */
      spawn_net_channel* ch = spawn_net_accept(ep);

      /* read the round id */
      spawn_net_read(ch, &round, sizeof(int));

      /* save the channel in our list */
      group->list_left[round] = ch;
  }

  return LWGRP_SUCCESS;
}

static int lwgrp_connect_right(lwgrp_chain* group, int round, const char* name)
{
  /* connect to specified name */
  spawn_net_channel* ch = spawn_net_connect(name);

  /* send the round id */
  spawn_net_write(ch, &round, sizeof(int));

  /* record the channel */
  group->list_right[round] = ch;

  return LWGRP_SUCCESS;
}

/* given a group, build a list of neighbors that are 2^d away on
 * our left and right sides */
static int lwgrp_chain_connect(lwgrp_chain* group)
{
  /* get our rank and size of the group */
  int64_t rank  = group->rank;
  int64_t ranks = group->size;

  /* build list of left and right ranks in our group */
  char* left  = strdup(group->left);
  char* right = strdup(group->right);
  int round = 0;
  int64_t dist = 1;
  while (dist < ranks) {
    /* define temps to hold next left/right addresses */
    char* next_right = NULL;
    char* next_left  = NULL;

    /* compute our relative rank for this round */
    int64_t relrank  = (rank >> round);

    if (relrank & 0x1) {
        /* we're an odd rank in this round */

        /* first, even ranks connect to right */
        if (rank - dist >= 0) {
          lwgrp_accept_left(group, round);
        }

        /* then, odd ranks connect to the right */
        if (rank + dist < ranks) {
          lwgrp_connect_right(group, round, right);
        }

        /* exchange data with left rank */
        if (rank - dist >= 0) {
          spawn_net_channel* ch = group->list_left[round];

          /* receive next left address from left */
          next_left = spawn_net_read_str(ch);

          /* send next right address to left */
          spawn_net_write_str(ch, right);
        }

        /* exchange data with right rank */
        if (rank + dist < ranks) {
          spawn_net_channel* ch = group->list_right[round];

          /* send next left address to right */
          spawn_net_write_str(ch, left);

          /* receive next right address from right */
          next_right = spawn_net_read_str(ch);
        }
    } else {
        /* we're an even rank in this round */

        /* first, even ranks connect to the right */
        if (rank + dist < ranks) {
          lwgrp_connect_right(group, round, right);
        }

        /* then, odd ranks connect to the right */
        if (rank - dist >= 0) {
          lwgrp_accept_left(group, round);
        }

        /* exchange data with right rank */
        if (rank + dist < ranks) {
          spawn_net_channel* ch = group->list_right[round];

          /* send next left address to right */
          spawn_net_write_str(ch, left);

          /* receive next right address from right */
          next_right = spawn_net_read_str(ch);
        }

        /* exchange data with left rank */
        if (rank - dist >= 0) {
          spawn_net_channel* ch = group->list_left[round];

          /* receive next left address from left */
          next_left = spawn_net_read_str(ch);

          /* send next right address to left */
          spawn_net_write_str(ch, right);
        }
    }

    if (left != NULL) {
      free(left);
      left = NULL;
    }
    if (right != NULL) {
      free(right);
      right = NULL;
    }
    left  = next_left;
    right = next_right;

    /* update our current left and right ranks to the next set */
    dist <<= 1;
    round++;
  }

  if (left != NULL) {
    free(left);
    left = NULL;
  }
  if (right != NULL) {
    free(right);
    right = NULL;
  }

  return LWGRP_SUCCESS;
}

static lwgrp_chain* lwgrp_chain_create(
  int64_t ranks,
  int64_t rank,
  const char* name,
  const char* left,
  const char* right,
  spawn_net_endpoint* ep)
{
  lwgrp_chain* group = (lwgrp_chain*) malloc(sizeof(lwgrp_chain));
  if (group == NULL) {
    return NULL;
  }

  /* copy input values from caller */
  group->size  = ranks;
  group->rank  = rank;
  group->name  = strdup(name);
  group->left  = strdup(left);
  group->right = strdup(right);
  group->ep    = ep;

  /* initialize the fields to 0 and NULL */
  group->list_size  = 0;
  group->list_left  = NULL;
  group->list_right = NULL;

  /* compute ceiling(log(ranks)) to determine the maximum
   * number of neighbors we can have */
  int64_t list_size = 0;
  int64_t count = 1;
  while (count < ranks) {
    list_size++;
    count <<= 1;
  }

  /* we grab one more than we need to store NULL at
   * end of each list */
  list_size++;

  /* allocate memory to hold list of left and right rank lists */
  spawn_net_channel** list_left  = NULL;
  spawn_net_channel** list_right = NULL;
  size_t list_bytes = list_size * sizeof(spawn_net_channel*);
  if (list_bytes > 0) {
    list_left  = (spawn_net_channel**) malloc(list_bytes);
    list_right = (spawn_net_channel**) malloc(list_bytes);
  }

  /* initialize our list of channels */
  int64_t i;
  for (i = 0; i < list_size; i++) {
    list_left[i]  = SPAWN_NET_CHANNEL_NULL;
    list_right[i] = SPAWN_NET_CHANNEL_NULL;
  }

  /* now that we've allocated memory, assign it to the list struct */
  group->list_size  = list_size;
  group->list_left  = list_left;
  group->list_right = list_right;

  /* create connections */
  lwgrp_chain_connect(group);

  return group;
}

static int lwgrp_chain_free(lwgrp_chain** pgroup)
{
  lwgrp_chain* group = *pgroup;

  /* disconnect all channels */
  int64_t i;
  for (i = 0; i < group->list_size; i++) {
    if (group->list_left[i] != SPAWN_NET_CHANNEL_NULL) {
      spawn_net_disconnect(&group->list_left[i]);
    }

    if (group->list_right[i] != SPAWN_NET_CHANNEL_NULL) {
      spawn_net_disconnect(&group->list_right[i]);
    }
  }

  /* free the list of channels */
  if (group->list_right != NULL) {
    free(group->list_right);
    group->list_right = NULL;
  }
  if (group->list_left != NULL) {
    free(group->list_left);
    group->list_left = NULL;
  }

  /* free the addresses */
  free(group->right);
  free(group->left);
  free(group->name);

  *pgroup = NULL;

  return LWGRP_SUCCESS;
}

static int lwgrp_chain_allgather_strmap(strmap* map, lwgrp_chain* group)
{
  int64_t rank  = group->rank;
  int64_t ranks = group->size;

  int round = 0;
  int64_t dist = 1;
  while (dist < ranks) {
    /* create temps to hold maps received from left and right */
    strmap* map_left = strmap_new();
    strmap* map_right = strmap_new();

    /* compute our relative rank for this round */
    int64_t relrank  = (rank >> round);

    if (relrank & 0x1) {
      /* we're odd in this round */

      /* exchange data with left partner */
      if (rank - dist >= 0) {
        /* recv then send */
        spawn_net_channel* ch = group->list_left[round];
        spawn_net_read_strmap(ch, map_left);
        spawn_net_write_strmap(ch, map);
      }

      /* exchange data with right partner */
      if (rank + dist < ranks) {
        /* send then recv */
        spawn_net_channel* ch = group->list_right[round];
        spawn_net_write_strmap(ch, map);
        spawn_net_read_strmap(ch, map_right);
      }
    } else {
      /* we're even in this round */

      /* exchange data with right partner */
      if (rank + dist < ranks) {
        /* send then recv */
        spawn_net_channel* ch = group->list_right[round];
        spawn_net_write_strmap(ch, map);
        spawn_net_read_strmap(ch, map_right);
      }

      /* exchange data with left partner */
      if (rank - dist >= 0) {
        /* recv then send */
        spawn_net_channel* ch = group->list_left[round];
        spawn_net_read_strmap(ch, map_left);
        spawn_net_write_strmap(ch, map);
      }
    }

    /* merge received maps with our current map */
    strmap_merge(map, map_left);
    strmap_merge(map, map_right);
    strmap_delete(&map_left);
    strmap_delete(&map_right);

    dist <<= 1;
    round++;
  }

  return LWGRP_SUCCESS;
}

int main(int argc, char* argv[])
{
  /* create address (e.g., open socket and encode as string) */
  spawn_net_endpoint* ep = spawn_net_open(SPAWN_NET_TYPE_IBUD);
  const char* ep_name = spawn_net_name(ep);

  /* get addresses of left and right procs */
  uint64_t rank, ranks;
  char *left, *right;
  int rc = ring_create(ep_name, &rank, &ranks, &left, &right);

#if 0
  /* print what we got */
  printf("Rank %d, Size %d, Left %s, Me %s, Right %s\n",
    rank, ranks, left, ep_name, right
  );
  fflush(stdout);
#endif

  /* create to log(N) tasks on left and right sides */
  lwgrp_chain* group = lwgrp_chain_create(ranks, rank, ep_name, left, right, ep);

  /* free left and right strings allocated in ring_create */
  free(left);
  free(right);

  /* gather endpoint addresses for all procs */
  strmap* map = strmap_new();
  strmap_setf(map, "%d=%s", rank, ep_name);

  lwgrp_chain_allgather_strmap(map, group);

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

  strmap_delete(&map);

  /* disconnect and free group */
  lwgrp_chain_free(&group);

  spawn_net_close(&ep);

  return 0;
}
