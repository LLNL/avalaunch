#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>

#include <sys/types.h>
#include <unistd.h>

#include "lwgrp.h"
#include "strmap.h"
#include "spawn_net.h"
#include "spawn_net_util.h"

/* waits for channel in specified round to connect */
static int lwgrp_accept_left(lwgrp* group, int target_round)
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

static int lwgrp_connect_right(lwgrp* group, int round, const char* name)
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
static int lwgrp_connect_blocking(lwgrp* group)
{
  /* get our rank and size of the group */
  int64_t rank  = group->rank;
  int64_t ranks = group->size;

  /* build list of left and right ranks in our group */
  char* left  = SPAWN_STRDUP(group->left);
  char* right = SPAWN_STRDUP(group->right);
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

    spawn_free(&left);
    spawn_free(&right);
    left  = next_left;
    right = next_right;

    /* update our current left and right ranks to the next set */
    dist <<= 1;
    round++;
  }

  spawn_free(&left);
  spawn_free(&right);

  return LWGRP_SUCCESS;
}

/* given a group, build a list of neighbors that are 2^d away on
 * our left and right sides */
static int lwgrp_connect(lwgrp* group)
{
  /* get our rank and size of the group */
  int64_t rank  = group->rank;
  int64_t ranks = group->size;

  /* build list of left and right ranks in our group */
  char* left  = SPAWN_STRDUP(group->left);
  char* right = SPAWN_STRDUP(group->right);
  int round = 0;
  int64_t dist = 1;
  while (dist < ranks) {
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
    }

    /* send next right address to left */
    if (rank - dist >= 0) {
        spawn_net_channel* ch = group->list_left[round];
        spawn_net_write_str(ch, right);
    }

    /* send next left address to right */
    if (rank + dist < ranks) {
        spawn_net_channel* ch = group->list_right[round];
        spawn_net_write_str(ch, left);
    }

    /* receive next left address from left */
    char* next_left = NULL;
    if (rank - dist >= 0) {
        spawn_net_channel* ch = group->list_left[round];
        next_left = spawn_net_read_str(ch);
    }

    /* receive next right address from right */
    char* next_right = NULL;
    if (rank + dist < ranks) {
        spawn_net_channel* ch = group->list_right[round];
        next_right = spawn_net_read_str(ch);
    }

    spawn_free(&left);
    spawn_free(&right);
    left  = next_left;
    right = next_right;

    /* update our current left and right ranks to the next set */
    dist <<= 1;
    round++;
  }

  spawn_free(&left);
  spawn_free(&right);

  return LWGRP_SUCCESS;
}

lwgrp* lwgrp_create(
  int64_t ranks,
  int64_t rank,
  const char* name,
  const char* left,
  const char* right,
  spawn_net_endpoint* ep)
{
  lwgrp* group = (lwgrp*) SPAWN_MALLOC(sizeof(lwgrp));

  /* copy input values from caller */
  group->size  = ranks;
  group->rank  = rank;
  group->name  = SPAWN_STRDUP(name);
  group->left  = SPAWN_STRDUP(left);
  group->right = SPAWN_STRDUP(right);
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
    list_left  = (spawn_net_channel**) SPAWN_MALLOC(list_bytes);
    list_right = (spawn_net_channel**) SPAWN_MALLOC(list_bytes);
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
  lwgrp_connect(group);

  return group;
}

int lwgrp_free(lwgrp** pgroup)
{
  lwgrp* group = *pgroup;

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
  spawn_free(&group->list_right);
  spawn_free(&group->list_left);

  /* free the addresses */
  spawn_free(&group->right);
  spawn_free(&group->left);
  spawn_free(&group->name);

  /* free the group */
  spawn_free(pgroup);

  return LWGRP_SUCCESS;
}

int64_t lwgrp_size(const lwgrp* group)
{
  int64_t size = 0;
  if (group != NULL) {
    size = group->size;
  }
  return size;
}

int64_t lwgrp_rank(const lwgrp* group)
{
  int64_t rank = -1;
  if (group != NULL) {
    rank = group->rank;
  }
  return rank;
}

/* TODO: need to unpack these values to convert them to right format */

/* compares first int,
 *   - used to compare color values after sorting
 *   - used to sort data back to its originating rank */
static int cmp_int(const void* a_void, const void* b_void, size_t offset)
{
  /* get pointers to the integer array, offset is dummy in this case */
  const int64_t* a = (const int64_t*) a_void;
  const int64_t* b = (const int64_t*) b_void;
  if (a[0] != b[0]) {
    if (a[0] > b[0]) {
      return 1;
    }
    return -1;
  }

  /* all values are equal if we make it here */
  return 0;
}

/* compares a (color,key,rank) integer tuple, first by color,
 * then key, then rank
 *   - used to sort (color,key,rank) tuples */
static int cmp_three_ints(const void* a_void, const void* b_void, size_t offset)
{
  /* get pointers to the integer array, offset is dummy in this case */
  const int64_t* a = (const int64_t*) a_void;
  const int64_t* b = (const int64_t*) b_void;

  /* compare color values first */
  if (a[0] != b[0]) {
    if (a[0] > b[0]) {
      return 1;
    }
    return -1;
  }

  /* then compare key values */
  if (a[1] != b[1]) {
    if (a[1] > b[1]) {
      return 1;
    }
    return -1;
  }

  /* finally compare ranks */
  if (a[2] != b[2]) {
    if (a[2] > b[2]) {
      return 1;
    }
    return -1;
  }

  /* all three are equal if we make it here */
  return 0;
}

/* compares a string
 *   - used to compare strings after sorting */
static int cmp_str(const void* a, const void* b, size_t offset)
{
  /* compare string values first */
  const char* a_str = (char*) a;
  const char* b_str = (char*) b;
  int rc = strcmp(a_str, b_str);
  if (rc != 0) {
    if (rc > 0) {
      return 1;
    }
    return -1;
  }

  /* all values are equal if we make it here */
  return 0;
}

/* compares a (string,rank) tuple, first by string,
 * then rank which is offset bytes from start of buffer
 *   - used to sort (string,rank) tuples */
static int cmp_str_int(const void* a, const void* b, size_t offset)
{
  /* compare string values first */
  const char* a_str = (char*) a;
  const char* b_str = (char*) b;
  int rc = strcmp(a_str, b_str);
  if (rc != 0) {
    if (rc > 0) {
      return 1;
    }
    return -1;
  }

  /* then compare int values, stored offset bytes from start of buffer */
  int64_t a_int = *(int64_t*)(a_str + offset);
  int64_t b_int = *(int64_t*)(b_str + offset);
  if (a_int != b_int) {
    if (a_int > b_int) {
      return 1;
    }
    return -1;
  }

  /* all values are equal if we make it here */
  return 0;
}

static int lwgrp_sort_bitonic_merge_blocking(
  void* value,
  void* scratch,
  size_t size,
  size_t offset,
  int (*compare)(const void*, const void*, size_t),
  int64_t start,
  int64_t num,
  int direction,
  const lwgrp* group)
{
  if (num > 1) {
    /* get our rank within the group */
    int64_t rank = group->rank;

    /* determine largest power of two that is smaller than num */
    int64_t count = 1;
    int64_t index = 0;
    while (count < num) {
      count <<= 1;
      index++;
    }
    count >>= 1;
    index--;

    /* divide range into two chunks, execute bitonic half-clean step,
     * then recursively merge each half */
    if (rank < start + count) {
      /* we are in the lower half, find a partner in the upper half */
      int64_t dst_rank = rank + count;
      if (dst_rank < start + num) {
        /* exchange data with our partner rank */
        spawn_net_channel* partner = group->list_right[index];
        spawn_net_read(partner, scratch, size);
        spawn_net_write(partner, value, size);

        /* select the appropriate value,
         * depedning on the sort direction */
        int cmp = (*compare)(scratch, value, offset);
        if ((direction && cmp < 0) || (!direction && cmp > 0)) {
          /* we keep the value if
           *   direction is ascending and new value is smaller,
           *   direction is descending and new value is greater */
          memcpy(value, scratch, size);
        }
      }

      /* recursively merge our half */
      lwgrp_sort_bitonic_merge_blocking(
        value, scratch, size, offset, compare,
        start, count, direction,
        group
      );
    } else {
      /* we are in the upper half, find a partner in the lower half */
      int64_t dst_rank = rank - count;
      if (dst_rank >= start) {
        /* exchange data with our partner rank */
        spawn_net_channel* partner = group->list_left[index];
        spawn_net_write(partner, value, size);
        spawn_net_read(partner, scratch, size);

        /* select the appropriate value,
         * depedning on the sort direction */
        int cmp = (*compare)(scratch, value, offset);
        if ((direction && cmp > 0) || (!direction && cmp < 0)) {
          /* we keep the value if
           *   direction is ascending and new value is bigger,
           *   direction is descending and new value is smaller */
          memcpy(value, scratch, size);
        }
      }

      /* recursively merge our half */
      int64_t new_start = start + count;
      int64_t new_num   = num - count;
      lwgrp_sort_bitonic_merge_blocking(
        value, scratch, size, offset, compare,
        new_start, new_num, direction,
        group
      );
    }
  }

  return 0;
}

static int lwgrp_sort_bitonic_merge(
  void* value,
  void* scratch,
  size_t size,
  size_t offset,
  int (*compare)(const void*, const void*, size_t),
  int64_t start,
  int64_t num,
  int direction,
  const lwgrp* group)
{
  if (num > 1) {
    /* get our rank within the group */
    int64_t rank = group->rank;

    /* determine largest power of two that is smaller than num */
    int64_t count = 1;
    int64_t index = 0;
    while (count < num) {
      count <<= 1;
      index++;
    }
    count >>= 1;
    index--;

    /* divide range into two chunks, execute bitonic half-clean step,
     * then recursively merge each half */
    if (rank < start + count) {
      /* we are in the lower half, find a partner in the upper half */
      int64_t dst_rank = rank + count;
      if (dst_rank < start + num) {
        /* exchange data with our partner rank */
        spawn_net_channel* partner = group->list_right[index];
        spawn_net_write(partner, value, size);
        spawn_net_read(partner, scratch, size);

        /* select the appropriate value,
         * depedning on the sort direction */
        int cmp = (*compare)(scratch, value, offset);
        if ((direction && cmp < 0) || (!direction && cmp > 0)) {
          /* we keep the value if
           *   direction is ascending and new value is smaller,
           *   direction is descending and new value is greater */
          memcpy(value, scratch, size);
        }
      }

      /* recursively merge our half */
      lwgrp_sort_bitonic_merge(
        value, scratch, size, offset, compare,
        start, count, direction,
        group
      );
    } else {
      /* we are in the upper half, find a partner in the lower half */
      int64_t dst_rank = rank - count;
      if (dst_rank >= start) {
        /* exchange data with our partner rank */
        spawn_net_channel* partner = group->list_left[index];
        spawn_net_write(partner, value, size);
        spawn_net_read(partner, scratch, size);

        /* select the appropriate value,
         * depedning on the sort direction */
        int cmp = (*compare)(scratch, value, offset);
        if ((direction && cmp > 0) || (!direction && cmp < 0)) {
          /* we keep the value if
           *   direction is ascending and new value is bigger,
           *   direction is descending and new value is smaller */
          memcpy(value, scratch, size);
        }
      }

      /* recursively merge our half */
      int64_t new_start = start + count;
      int64_t new_num   = num - count;
      lwgrp_sort_bitonic_merge(
        value, scratch, size, offset, compare,
        new_start, new_num, direction,
        group
      );
    }
  }

  return 0;
}

static int lwgrp_sort_bitonic_sort(
  void* value,
  void* scratch,
  size_t size,
  size_t offset,
  int (*compare)(const void*, const void*, size_t),
  int64_t start,
  int64_t num,
  int direction,
  const lwgrp* group)
{
  if (num > 1) {
    /* get our rank in our group */
    int rank = group->rank;

    /* recursively divide and sort each half */
    int64_t mid = num / 2;
    if (rank < start + mid) {
      /* sort first half in one direction */
      lwgrp_sort_bitonic_sort(
        value, scratch, size, offset, compare,
        start, mid, !direction,
        group
      );
    } else {
      /* sort the second half in the other direction */
      int64_t new_start = start + mid;
      int64_t new_num   = num - mid;
      lwgrp_sort_bitonic_sort(
        value, scratch, size, offset, compare,
        new_start, new_num, direction,
        group
      );
    }

    /* merge the two sorted halves */
    lwgrp_sort_bitonic_merge(
      value, scratch, size, offset, compare,
      start, num, direction,
      group
    );
  }

  return 0;
}

/* globally sort items across processes in group,
 * each process provides its tuple as item on input,
 * on output item is overwritten with a new item
 * such that if rank_i < rank_j, item_i < item_j for all i and j */
static int lwgrp_sort_bitonic(
  void* value,
  size_t size,
  size_t offset,
  int (*compare)(const void*, const void*, size_t),
  const lwgrp* group)
{
  /* allocate a scratch buffer to hold received type during sort */
  void* scratch = SPAWN_MALLOC(size);

  /* conduct the bitonic sort on our values */
  int64_t ranks = group->size;
  int rc = lwgrp_sort_bitonic_sort(
    value, scratch, size, offset, compare,
    0, ranks, 1,
    group
  );

  /* free the buffer */
  spawn_free(&scratch);

  return rc;
}

enum scan_fields {
  SCAN_COLOR = 0, /* running count of number of groups */
  SCAN_FLAG  = 1, /* set flag to 1 when we should stop accumulating */
  SCAN_COUNT = 2, /* running count of ranks within segmented group */
  SCAN_NEXT  = 3, /* address of next process to talk to */
};

enum chain_fields {
  CHAIN_SRC   = 0, /* address of originating rank */
  CHAIN_RANK  = 1, /* rank of originating process within its new group */
  CHAIN_SIZE  = 2, /* size of new group */
  CHAIN_ID    = 3, /* id of new group */
  CHAIN_COUNT = 4, /* number of new groups */
};

/* assumes that color/key/rank tuples have been globally sorted
 * across ranks of in chain, computes corresponding group
 * information for val and passes that back to originating rank:
 *   1) determines group boundaries and left and right neighbors
 *      by sending pt2pt msgs to left and right neighbors and
 *      comparing color values
 *   2) executes left-to-right and right-to-left (double) inclusive
 *      segmented scan to compute number of ranks to left and right
 *      sides of host value */
static lwgrp* lwgrp_split_sorted_blocking(
  const void* value,
  size_t size,
  size_t offset,
  int (*compare)(const void*, const void*, size_t),
  const lwgrp* group)
{
  /* we will fill in 7 values (src, rank, size, groupid, groups, left, right)
   * representing the chain data structure for the the globally
   * ordered color/key/rank tuple that we hold, which we'll later
   * send back to the rank that contributed our item */
  size_t payload_offset = offset + sizeof(int64_t);
  size_t payload_size = size - payload_offset;
  size_t result_buf_size = 5 * sizeof(int64_t) + 2 * payload_size;
  void* result_buf = SPAWN_MALLOC(result_buf_size);
  int64_t* send_ints = (int64_t*) result_buf;

  /* TODO: unpack */
  /* record address of process that contributed this item */
  int64_t* orig_ptr = (int64_t*)((char*)value + offset);
  send_ints[CHAIN_SRC] = *orig_ptr;

  /* allocate a scratch buffer to receive value from left neighbor */
  void* left_buf = SPAWN_MALLOC(size);

  /* allocate a scratch buffer to receive value from right neighbor */
  void* right_buf = SPAWN_MALLOC(size);

  /* exchange data with left and right neighbors to find
   * boundaries of group */
  int64_t rank  = group->rank;
  int64_t ranks = group->size;
  if (rank & 0x1) {
    /* odd ranks recv from left first, then send to right */
    if (rank > 0) {
      /* recv from left, then send to left */
      spawn_net_channel* left = group->list_left[0];
      spawn_net_read(left, left_buf, size);
      spawn_net_write(left, value, size);
    }
    if (rank < ranks - 1) {
      /* send to right, then recv from right */
      spawn_net_channel* right = group->list_right[0];
      spawn_net_write(right, value, size);
      spawn_net_read(right, right_buf, size);
    }
  } else {
    /* even ranks send right first, then recv from left */
    if (rank < ranks - 1) {
      /* send to right, then recv from right */
      spawn_net_channel* right = group->list_right[0];
      spawn_net_write(right, value, size);
      spawn_net_read(right, right_buf, size);
    }
    if (rank > 0) {
      /* recv from left, then send to left */
      spawn_net_channel* left = group->list_left[0];
      spawn_net_read(left, left_buf, size);
      spawn_net_write(left, value, size);
    }
  }

  /* if we have a left neighbor, and if his color value matches ours,
   * then our element is part of his group, otherwise we are the first
   * rank of a new group */
  int first_in_group = 1;
  if (rank > 0) {
    /* copy endpoint name from neighbor */
    if (payload_size > 0) {
      char* payload = ((char*)left_buf + payload_offset);
      char* bufptr = ((char*)result_buf + 5 * sizeof(int64_t));
      memcpy(bufptr, payload, payload_size);
    }

    /* check whether we're first in a new group */
    int left_cmp = (*compare)(left_buf, value, 0);
    if (left_cmp == 0) {
      first_in_group = 0;
    }
  }

  /* if we have a right neighbor, and if his color value matches ours,
   * then our element is part of his group, otherwise we are the last
   * rank of our group */
  int last_in_group = 1;
  if (rank < ranks - 1) {
    /* copy endpoint name from neighbor */
    if (payload_size > 0) {
      char* payload = ((char*)right_buf + payload_offset);
      char* bufptr = ((char*)result_buf + 5 * sizeof(int64_t) + payload_size);
      memcpy(bufptr, payload, payload_size);
    }

    /* check whether we're last in our group */
    int right_cmp = (*compare)(right_buf, value, 0);
    if (right_cmp == 0) {
      last_in_group = 0;
    }
  }

  /* free temporary buffers */
  spawn_free(&left_buf);
  spawn_free(&right_buf);

  /* TODO: pack */
  /* prepare buffers for our scan operations:
   * group count, flag, rank count, next proc */
  size_t scan_size = 3 * sizeof(int64_t);
  int64_t send_left_ints[3]  = {0,0,1};
  int64_t send_right_ints[3] = {0,0,1};
  int64_t recv_left_ints[3]  = {0,0,0};
  int64_t recv_right_ints[3] = {0,0,0};
  if (first_in_group) {
    send_right_ints[SCAN_COLOR] = 1;
    send_right_ints[SCAN_FLAG] = 1;
  }
  if (last_in_group) {
    send_left_ints[SCAN_COLOR] = 1;
    send_left_ints[SCAN_FLAG] = 1;
  }

  /* execute inclusive scan in both directions to count number of
   * ranks in our group to our left and right sides */
  int64_t dist  = 1;
  int64_t index = 0;
  while (dist < ranks) {
    /* compute our relative rank for this round */
    int64_t relrank  = (rank >> index);

    /* send and receive data with left partner */
    if (relrank & 0x1) {
      /* odd ranks recv from left first, then send to right */
      if (rank - dist >= 0) {
        spawn_net_channel* partner = group->list_left[index];
        spawn_net_read(partner, recv_left_ints, scan_size);
        spawn_net_write(partner, send_left_ints, scan_size);
      }
      if (rank + dist < ranks) {
        spawn_net_channel* partner = group->list_right[index];
        spawn_net_write(partner, send_right_ints, scan_size);
        spawn_net_read(partner, recv_right_ints, scan_size);
      }
    } else {
      /* even ranks send right first, then recv from left */
      if (rank + dist < ranks) {
        spawn_net_channel* partner = group->list_right[index];
        spawn_net_write(partner, send_right_ints, scan_size);
        spawn_net_read(partner, recv_right_ints, scan_size);
      }
      if (rank - dist >= 0) {
        spawn_net_channel* partner = group->list_left[index];
        spawn_net_read(partner, recv_left_ints, scan_size);
        spawn_net_write(partner, send_left_ints, scan_size);
      }
    }

    /* TODO: unpack */
    /* reduce data from left partner */
    if (rank - dist >= 0) {
      /* count the number of groups to our left */
      send_right_ints[SCAN_COLOR] += recv_left_ints[SCAN_COLOR];

      /* continue accumulating the count in our right-going data
       * if our flag has not already been set */
      if (send_right_ints[SCAN_FLAG] != 1) {
        send_right_ints[SCAN_FLAG]   = recv_left_ints[SCAN_FLAG];
        send_right_ints[SCAN_COUNT] += recv_left_ints[SCAN_COUNT];
      }
    }

    /* reduce data from right partner */
    if (rank + dist < ranks) {
      /* count the number of groups to our right */
      send_left_ints[SCAN_COLOR] += recv_right_ints[SCAN_COLOR];

      /* continue accumulating the count in our left-going data
       * if our flag has not already been set */
      if (send_left_ints[SCAN_FLAG] != 1) {
        send_left_ints[SCAN_FLAG]   = recv_right_ints[SCAN_FLAG];
        send_left_ints[SCAN_COUNT] += recv_right_ints[SCAN_COUNT];
      }
    }

    /* prepare for the next round */
    dist <<= 1;
    index++;
  }

  /* Now we can set our rank and the number of ranks in our group.
   * At this point, our right-going count is the number of ranks to our
   * left including ourself, and the left-going count is the number of
   * ranks to our right including ourself.
   * Our rank is the number of ranks to our left (right-going count
   * minus 1), and the group size is the sum of right-going and
   * left-going counts minus 1 so we don't double counts ourself. */
  send_ints[CHAIN_RANK] = send_right_ints[SCAN_COUNT] - 1;
  send_ints[CHAIN_SIZE] = send_right_ints[SCAN_COUNT] + 
                          send_left_ints[SCAN_COUNT] - 1;
  send_ints[CHAIN_ID]    = send_right_ints[SCAN_COLOR] - 1;
  send_ints[CHAIN_COUNT] = send_right_ints[SCAN_COLOR] +
                           send_left_ints[SCAN_COLOR] - 1;

  /* sort item back to its originating rank */
  lwgrp_sort_bitonic(
    result_buf, result_buf_size, 0, cmp_int, group
  );

  /* build new group */
  int64_t newrank  = send_ints[CHAIN_RANK];
  int64_t newranks = send_ints[CHAIN_SIZE];
  char* newleft    = NULL;
  char* newright   = NULL;
  if (newrank > 0) {
    newleft = (char*)result_buf + 5 * sizeof(int64_t);
  }
  if (newrank < newranks - 1) {
    newright = (char*)result_buf + 5 * sizeof(int64_t) + payload_size;
  }
  lwgrp* newgroup = lwgrp_create(
    newranks, newrank, group->name, newleft, newright, group->ep
  );

  /* free buffer */
  spawn_free(&result_buf);

  return newgroup;
}

static lwgrp* lwgrp_split_sorted(
  const void* value,
  size_t size,
  size_t offset,
  int (*compare)(const void*, const void*, size_t),
  const lwgrp* group)
{
  /* we will fill in 7 values (src, rank, size, groupid, groups, left, right)
   * representing the chain data structure for the the globally
   * ordered color/key/rank tuple that we hold, which we'll later
   * send back to the rank that contributed our item */
  size_t payload_offset = offset + sizeof(int64_t);
  size_t payload_size = size - payload_offset;
  size_t result_buf_size = 5 * sizeof(int64_t) + 2 * payload_size;
  void* result_buf = SPAWN_MALLOC(result_buf_size);
  int64_t* send_ints = (int64_t*) result_buf;

  /* TODO: unpack */
  /* record address of process that contributed this item */
  int64_t* orig_ptr = (int64_t*)((char*)value + offset);
  send_ints[CHAIN_SRC] = *orig_ptr;

  /* allocate a scratch buffer to receive value from left neighbor */
  void* left_buf = SPAWN_MALLOC(size);

  /* allocate a scratch buffer to receive value from right neighbor */
  void* right_buf = SPAWN_MALLOC(size);

  /* exchange data with left and right neighbors to find
   * boundaries of group */
  int64_t rank  = group->rank;
  int64_t ranks = group->size;

  /* send data left */
  if (rank > 0) {
    spawn_net_channel* left = group->list_left[0];
    spawn_net_write(left, value, size);
  }

  /* send data right */
  if (rank < ranks - 1) {
    spawn_net_channel* right = group->list_right[0];
    spawn_net_write(right, value, size);
  }

  /* read data from left */
  if (rank > 0) {
    spawn_net_channel* left = group->list_left[0];
    spawn_net_read(left, left_buf, size);
  }

  /* recv data from right */
  if (rank < ranks - 1) {
    spawn_net_channel* right = group->list_right[0];
    spawn_net_read(right, right_buf, size);
  }

  /* if we have a left neighbor, and if his color value matches ours,
   * then our element is part of his group, otherwise we are the first
   * rank of a new group */
  int first_in_group = 1;
  if (rank > 0) {
    /* copy endpoint name from neighbor */
    if (payload_size > 0) {
      char* payload = ((char*)left_buf + payload_offset);
      char* bufptr = ((char*)result_buf + 5 * sizeof(int64_t));
      memcpy(bufptr, payload, payload_size);
    }

    /* check whether we're first in a new group */
    int left_cmp = (*compare)(left_buf, value, 0);
    if (left_cmp == 0) {
      first_in_group = 0;
    }
  }

  /* if we have a right neighbor, and if his color value matches ours,
   * then our element is part of his group, otherwise we are the last
   * rank of our group */
  int last_in_group = 1;
  if (rank < ranks - 1) {
    /* copy endpoint name from neighbor */
    if (payload_size > 0) {
      char* payload = ((char*)right_buf + payload_offset);
      char* bufptr = ((char*)result_buf + 5 * sizeof(int64_t) + payload_size);
      memcpy(bufptr, payload, payload_size);
    }

    /* check whether we're last in our group */
    int right_cmp = (*compare)(right_buf, value, 0);
    if (right_cmp == 0) {
      last_in_group = 0;
    }
  }

  /* free temporary buffers */
  spawn_free(&left_buf);
  spawn_free(&right_buf);

  /* TODO: pack */
  /* prepare buffers for our scan operations:
   * group count, flag, rank count, next proc */
  size_t scan_size = 3 * sizeof(int64_t);
  int64_t send_left_ints[3]  = {0,0,1};
  int64_t send_right_ints[3] = {0,0,1};
  int64_t recv_left_ints[3]  = {0,0,0};
  int64_t recv_right_ints[3] = {0,0,0};
  if (first_in_group) {
    send_right_ints[SCAN_COLOR] = 1;
    send_right_ints[SCAN_FLAG] = 1;
  }
  if (last_in_group) {
    send_left_ints[SCAN_COLOR] = 1;
    send_left_ints[SCAN_FLAG] = 1;
  }

  /* execute inclusive scan in both directions to count number of
   * ranks in our group to our left and right sides */
  int64_t dist  = 1;
  int64_t index = 0;
  while (dist < ranks) {
    /* send data left */
    if (rank - dist >= 0) {
      spawn_net_channel* partner = group->list_left[index];
      spawn_net_write(partner, send_left_ints, scan_size);
    }

    /* send data right */
    if (rank + dist < ranks) {
      spawn_net_channel* partner = group->list_right[index];
      spawn_net_write(partner, send_right_ints, scan_size);
    }

    /* recv data from left */
    if (rank - dist >= 0) {
      spawn_net_channel* partner = group->list_left[index];
      spawn_net_read(partner, recv_left_ints, scan_size);
    }

    /* recv data from right */
    if (rank + dist < ranks) {
      spawn_net_channel* partner = group->list_right[index];
      spawn_net_read(partner, recv_right_ints, scan_size);
    }

    /* TODO: unpack */
    /* reduce data from left partner */
    if (rank - dist >= 0) {
      /* count the number of groups to our left */
      send_right_ints[SCAN_COLOR] += recv_left_ints[SCAN_COLOR];

      /* continue accumulating the count in our right-going data
       * if our flag has not already been set */
      if (send_right_ints[SCAN_FLAG] != 1) {
        send_right_ints[SCAN_FLAG]   = recv_left_ints[SCAN_FLAG];
        send_right_ints[SCAN_COUNT] += recv_left_ints[SCAN_COUNT];
      }
    }

    /* reduce data from right partner */
    if (rank + dist < ranks) {
      /* count the number of groups to our right */
      send_left_ints[SCAN_COLOR] += recv_right_ints[SCAN_COLOR];

      /* continue accumulating the count in our left-going data
       * if our flag has not already been set */
      if (send_left_ints[SCAN_FLAG] != 1) {
        send_left_ints[SCAN_FLAG]   = recv_right_ints[SCAN_FLAG];
        send_left_ints[SCAN_COUNT] += recv_right_ints[SCAN_COUNT];
      }
    }

    /* prepare for the next round */
    dist <<= 1;
    index++;
  }

  /* Now we can set our rank and the number of ranks in our group.
   * At this point, our right-going count is the number of ranks to our
   * left including ourself, and the left-going count is the number of
   * ranks to our right including ourself.
   * Our rank is the number of ranks to our left (right-going count
   * minus 1), and the group size is the sum of right-going and
   * left-going counts minus 1 so we don't double counts ourself. */
  send_ints[CHAIN_RANK] = send_right_ints[SCAN_COUNT] - 1;
  send_ints[CHAIN_SIZE] = send_right_ints[SCAN_COUNT] + 
                          send_left_ints[SCAN_COUNT] - 1;
  send_ints[CHAIN_ID]    = send_right_ints[SCAN_COLOR] - 1;
  send_ints[CHAIN_COUNT] = send_right_ints[SCAN_COLOR] +
                           send_left_ints[SCAN_COLOR] - 1;

  /* sort item back to its originating rank */
  lwgrp_sort_bitonic(
    result_buf, result_buf_size, 0, cmp_int, group
  );

  /* build new group */
  int64_t newrank  = send_ints[CHAIN_RANK];
  int64_t newranks = send_ints[CHAIN_SIZE];
  char* newleft    = NULL;
  char* newright   = NULL;
  if (newrank > 0) {
    newleft = (char*)result_buf + 5 * sizeof(int64_t);
  }
  if (newrank < newranks - 1) {
    newright = (char*)result_buf + 5 * sizeof(int64_t) + payload_size;
  }
  lwgrp* newgroup = lwgrp_create(
    newranks, newrank, group->name, newleft, newright, group->ep
  );

  /* free buffer */
  spawn_free(&result_buf);

  return newgroup;
}

lwgrp* lwgrp_split(
  const lwgrp* comm,
  int64_t color,
  int64_t key)
{
  /* TODO: for small groups, fastest to do an allgather and
   * local sort */

  /* TODO: allreduce to determine whether keys are already ordered and
   * to compute min and max color values, if already ordered, reduce
   * problem to bin split using min/max colors to set number of bins */

  /* TODO: prepare input data (need allreduce on ep lengths),
   * convert color/key to 64-bit values */

  /* determine maximum length of endpoint names */
  uint64_t length = (uint64_t) (strlen(comm->name) + 1);
  lwgrp_allreduce_uint64_max(&length, 1, comm);
  size_t max_name = length;

  /* allocate memory to hold item for sorting (color,key,rank) tuple
   * and prepare input -- O(1) local */
  size_t buf_size = 3 * sizeof(int64_t) + max_name;
  char* buf = SPAWN_MALLOC(buf_size);

  /* TODO: we should pack these to be network order */

  /* copy in our color, key, and rank tuple */
  int64_t* ptr = (int64_t*) buf;
  ptr[0] = color;
  ptr[1] = key;
  ptr[2] = comm->rank;

  /* copy in endpoint name */
  char* ptr_ep = buf + 3 * sizeof(int64_t);
  strcpy(ptr_ep, comm->name);

  /* sort our values using bitonic sort algorithm -- 
   * O(log^2 N) communication */
  lwgrp_sort_bitonic(
    buf, buf_size, 0, cmp_three_ints, comm
  );

  /* now split our sorted values by comparing our value with our
   * left and right neighbors to determine group boundaries --
   * O(log N) communication, need offset to skip color and key
   * and point to rank (of type int64_t) followed by endpoint name */
  size_t offset = 2 * sizeof(int64_t);
  lwgrp* newgroup = lwgrp_split_sorted(
    buf, buf_size, offset, cmp_int, comm
  );

  /* free memory allocated for buffer */
  spawn_free(&buf);

  return newgroup;
}

/* int lwgrp_split_str(MPI_Comm comm, const void* str, int* groups, int* groupid)
 *   IN  comm    - input communicator (handle)
 *   IN  str     - string (NUL-terminated string)
 *   OUT groups  - number of unique strings in comm (non-negative integer)
 *   OUT groupid - id for specified string (non-negative integer)
 *
 * Given an arbitrary-length string on each process, return the number
 * of unique strings, and assign a unique id to each.
 *
 * rank str    groups groupid
 * 0    hello  2      0
 * 1    world  2      1
 * 2    world  2      1
 * 3    world  2      1
 * 4    hello  2      0
 * 5    world  2      1
 * 6    hello  2      0
 * 7    hello  2      0
 *
 * This function computes the total number of unique strings
 * when taking the union of the strings from all processes in comm.
 * Each string is assigned a unique id from 0 to M-1 in groupid,
 * where M is the number of unique strings.
 * The groupid value is the same on two different processes
 * if and only if both processes specify the same string.
 * This groupid can be used as a color value in MPI_COMM_SPLIT. */
lwgrp* lwgrp_split_str(const lwgrp* comm, const char* str)
{
  /* require str not be NULL */
  if (str == NULL) {
    /* TODO: error */
  }

  /* get max length of string and endpoint names */
  uint64_t lengths[2];
  lengths[0] = (uint64_t) (strlen(str) + 1);
  lengths[1] = (uint64_t) (strlen(comm->name) + 1);
  lwgrp_allreduce_uint64_max(lengths, 2, comm);
  uint64_t max_len  = lengths[0];
  uint64_t max_name = lengths[1];

  /* allocate space to hold a copy of the string,
   * our rank, and our endpoint name */
  size_t buf_size = max_len + sizeof(int64_t) + max_name;
  void* buf = SPAWN_MALLOC(buf_size);

  /* Prepare buffer, copy in string and then rank after max_len characters.
   * This rank serves two purposes. First by sorting on string and then rank,
   * it ensures that every item is unique since the ranks are distinct.
   * Second, it is used as a return address to send the result back. */

  /* copy string provided by caller */
  char* ptr = (char*) buf;
  strcpy(ptr, str);
  ptr += max_len;

  /* copy our rank in the current group */
  int64_t* ptr_int64 = (int64_t*) ptr;
  *ptr_int64 = comm->rank;
  ptr += sizeof(int64_t);

  /* copy in endpoint name */
  strcpy(ptr, comm->name);
  ptr += max_name;

  /* sort our values using bitonic sort algorithm -- 
   * O(log^2 N) communication */
  lwgrp_sort_bitonic(
    buf, buf_size, max_len, cmp_str_int, comm
  );

  /* now split our sorted values by comparing our value with our
   * left and right neighbors to determine group boundaries --
   * O(log N) communication */
  lwgrp* newgroup = lwgrp_split_sorted(
    buf, buf_size, max_len, cmp_str, comm
  );

  /* free memory allocated for buffer */
  spawn_free(&buf);

  return newgroup;
}

int lwgrp_shift_blocking(
  const void* buf,
  void* left,
  void* right,
  size_t buf_size,
  const lwgrp* group)
{
  int64_t rank  = group->rank;
  int64_t ranks = group->size;

  if (rank & 0x1) {
    /* we're an odd rank */

    /* exchange data with left partner */
    /* recv then send */
    if (rank - 1 >= 0) {
      spawn_net_channel* ch = group->list_left[0];
      spawn_net_read(ch, left, buf_size);
      spawn_net_write(ch, buf, buf_size);
    }

    /* exchange data with right partner */
    /* send then recv */
    if (rank + 1 < ranks) {
      spawn_net_channel* ch = group->list_right[0];
      spawn_net_write(ch, buf, buf_size);
      spawn_net_read(ch, right, buf_size);
    }
  } else {
    /* we're an even rank */

    /* exchange data with right partner */
    /* send then recv */
    if (rank + 1 < ranks) {
      spawn_net_channel* ch = group->list_right[0];
      spawn_net_write(ch, buf, buf_size);
      spawn_net_read(ch, right, buf_size);
    }

    /* exchange data with left partner */
    /* recv then send */
    if (rank - 1 >= 0) {
      spawn_net_channel* ch = group->list_left[0];
      spawn_net_read(ch, left, buf_size);
      spawn_net_write(ch, buf, buf_size);
    }
  }

  return LWGRP_SUCCESS;
}

int lwgrp_shift(
  const void* buf,
  void* left,
  void* right,
  size_t buf_size,
  const lwgrp* group)
{
  int64_t rank  = group->rank;
  int64_t ranks = group->size;

  /* send data left */
  if (rank > 0) {
    spawn_net_channel* ch = group->list_left[0];
    spawn_net_write(ch, buf, buf_size);
  }

  /* send data right */
  if (rank < ranks - 1) {
    spawn_net_channel* ch = group->list_right[0];
    spawn_net_write(ch, buf, buf_size);
  }

  /* recv data from left */
  if (rank > 0) {
    spawn_net_channel* ch = group->list_left[0];
    spawn_net_read(ch, left, buf_size);
  }

  /* recv data from right */
  if (rank < ranks - 1) {
    spawn_net_channel* ch = group->list_right[0];
    spawn_net_read(ch, right, buf_size);
  }

  return LWGRP_SUCCESS;
}

int lwgrp_barrier_blocking(const lwgrp* group)
{
  int64_t rank  = group->rank;
  int64_t ranks = group->size;

  char c = 'A';
  void* buf = (void*) &c;
  size_t buf_size = 1;

  int round = 0;
  int64_t dist = 1;
  while (dist < ranks) {
    /* compute our relative rank for this round */
    int64_t relrank  = (rank >> round);

    if (relrank & 0x1) {
      /* we're odd in this round */

      /* exchange data with left partner */
      if (rank - dist >= 0) {
        /* recv then send */
        spawn_net_channel* ch = group->list_left[round];
        spawn_net_read(ch, buf, buf_size);
        spawn_net_write(ch, buf, buf_size);
      }

      /* exchange data with right partner */
      if (rank + dist < ranks) {
        /* send then recv */
        spawn_net_channel* ch = group->list_right[round];
        spawn_net_write(ch, buf, buf_size);
        spawn_net_read(ch, buf, buf_size);
      }
    } else {
      /* we're even in this round */

      /* exchange data with right partner */
      if (rank + dist < ranks) {
        /* send then recv */
        spawn_net_channel* ch = group->list_right[round];
        spawn_net_write(ch, buf, buf_size);
        spawn_net_read(ch, buf, buf_size);
      }

      /* exchange data with left partner */
      if (rank - dist >= 0) {
        /* recv then send */
        spawn_net_channel* ch = group->list_left[round];
        spawn_net_read(ch, buf, buf_size);
        spawn_net_write(ch, buf, buf_size);
      }
    }

    dist <<= 1;
    round++;
  }

  return LWGRP_SUCCESS;
}

int lwgrp_barrier(const lwgrp* group)
{
  int64_t rank  = group->rank;
  int64_t ranks = group->size;

  char c = 'A';
  void* buf = (void*) &c;
  size_t buf_size = 1;

  int round = 0;
  int64_t dist = 1;
  while (dist < ranks) {
    /* send message left */
    if (rank - dist >= 0) {
      spawn_net_channel* ch = group->list_left[round];
      spawn_net_write(ch, buf, buf_size);
    }

    /* send message right */
    if (rank + dist < ranks) {
      spawn_net_channel* ch = group->list_right[round];
      spawn_net_write(ch, buf, buf_size);
    }

    /* recv message from left */
    if (rank - dist >= 0) {
      spawn_net_channel* ch = group->list_left[round];
      spawn_net_read(ch, buf, buf_size);
    }

    /* recv message from right */
    if (rank + dist < ranks) {
      spawn_net_channel* ch = group->list_right[round];
      spawn_net_read(ch, buf, buf_size);
    }

    dist <<= 1;
    round++;
  }

  return LWGRP_SUCCESS;
}

int lwgrp_allreduce_uint64_sum_blocking(uint64_t* buf, uint64_t count, const lwgrp* group)
{
  int64_t rank  = group->rank;
  int64_t ranks = group->size;

  size_t buf_size = count * sizeof(uint64_t);
  uint64_t* left_buf  = SPAWN_MALLOC(buf_size);
  uint64_t* right_buf = SPAWN_MALLOC(buf_size);

  int i;
  int round = 0;
  int64_t dist = 1;
  while (dist < ranks) {
    /* compute our relative rank for this round */
    int64_t relrank  = (rank >> round);

    if (relrank & 0x1) {
      /* we're odd in this round */

      /* exchange data with left partner */
      if (rank - dist >= 0) {
        /* recv then send */
        spawn_net_channel* ch = group->list_left[round];
        spawn_net_read(ch, left_buf, buf_size);
        spawn_net_write(ch, buf, buf_size);
      }

      /* exchange data with right partner */
      if (rank + dist < ranks) {
        /* send then recv */
        spawn_net_channel* ch = group->list_right[round];
        spawn_net_write(ch, buf, buf_size);
        spawn_net_read(ch, right_buf, buf_size);
      }
    } else {
      /* we're even in this round */

      /* exchange data with right partner */
      if (rank + dist < ranks) {
        /* send then recv */
        spawn_net_channel* ch = group->list_right[round];
        spawn_net_write(ch, buf, buf_size);
        spawn_net_read(ch, right_buf, buf_size);
      }

      /* exchange data with left partner */
      if (rank - dist >= 0) {
        /* recv then send */
        spawn_net_channel* ch = group->list_left[round];
        spawn_net_read(ch, left_buf, buf_size);
        spawn_net_write(ch, buf, buf_size);
      }
    }

    /* merge received maps with our current map */
    if (rank - dist >= 0) {
      for (i = 0; i < count; i++) {
        buf[i] += left_buf[i];
      }
    }
    if (rank + dist < ranks) {
      for (i = 0; i < count; i++) {
        buf[i] += right_buf[i];
      }
    }

    dist <<= 1;
    round++;
  }

  spawn_free(&right_buf);
  spawn_free(&left_buf);

  return LWGRP_SUCCESS;
}

int lwgrp_allreduce_uint64_sum(uint64_t* buf, uint64_t count, const lwgrp* group)
{
  int64_t rank  = group->rank;
  int64_t ranks = group->size;

  size_t buf_size = count * sizeof(uint64_t);
  uint64_t* left_buf  = SPAWN_MALLOC(buf_size);
  uint64_t* right_buf = SPAWN_MALLOC(buf_size);

  int i;
  int round = 0;
  int64_t dist = 1;
  while (dist < ranks) {
    /* send message left */
    if (rank - dist >= 0) {
      spawn_net_channel* ch = group->list_left[round];
      spawn_net_write(ch, buf, buf_size);
    }

    /* send message right */
    if (rank + dist < ranks) {
      spawn_net_channel* ch = group->list_right[round];
      spawn_net_write(ch, buf, buf_size);
    }

    /* recv message from left */
    if (rank - dist >= 0) {
      spawn_net_channel* ch = group->list_left[round];
      spawn_net_read(ch, left_buf, buf_size);
    }

    /* recv message from right */
    if (rank + dist < ranks) {
      spawn_net_channel* ch = group->list_right[round];
      spawn_net_read(ch, right_buf, buf_size);
    }

    /* merge received maps with our current map */
    if (rank - dist >= 0) {
      for (i = 0; i < count; i++) {
        buf[i] += left_buf[i];
      }
    }
    if (rank + dist < ranks) {
      for (i = 0; i < count; i++) {
        buf[i] += right_buf[i];
      }
    }

    dist <<= 1;
    round++;
  }

  spawn_free(&right_buf);
  spawn_free(&left_buf);

  return LWGRP_SUCCESS;
}

int lwgrp_allreduce_uint64_max_blocking(uint64_t* buf, uint64_t count, const lwgrp* group)
{
  int64_t rank  = group->rank;
  int64_t ranks = group->size;

  size_t buf_size = count * sizeof(uint64_t);
  uint64_t* left_buf  = SPAWN_MALLOC(buf_size);
  uint64_t* right_buf = SPAWN_MALLOC(buf_size);

  int i;
  int round = 0;
  int64_t dist = 1;
  while (dist < ranks) {
    /* compute our relative rank for this round */
    int64_t relrank  = (rank >> round);

    if (relrank & 0x1) {
      /* we're odd in this round */

      /* exchange data with left partner */
      if (rank - dist >= 0) {
        /* recv then send */
        spawn_net_channel* ch = group->list_left[round];
        spawn_net_read(ch, left_buf, buf_size);
        spawn_net_write(ch, buf, buf_size);
      }

      /* exchange data with right partner */
      if (rank + dist < ranks) {
        /* send then recv */
        spawn_net_channel* ch = group->list_right[round];
        spawn_net_write(ch, buf, buf_size);
        spawn_net_read(ch, right_buf, buf_size);
      }
    } else {
      /* we're even in this round */

      /* exchange data with right partner */
      if (rank + dist < ranks) {
        /* send then recv */
        spawn_net_channel* ch = group->list_right[round];
        spawn_net_write(ch, buf, buf_size);
        spawn_net_read(ch, right_buf, buf_size);
      }

      /* exchange data with left partner */
      if (rank - dist >= 0) {
        /* recv then send */
        spawn_net_channel* ch = group->list_left[round];
        spawn_net_read(ch, left_buf, buf_size);
        spawn_net_write(ch, buf, buf_size);
      }
    }

    /* merge received maps with our current map */
    if (rank - dist >= 0) {
      for (i = 0; i < count; i++) {
        if (left_buf[i] > buf[i]) {
          buf[i] = left_buf[i];
        }
      }
    }
    if (rank + dist < ranks) {
      for (i = 0; i < count; i++) {
        if (right_buf[i] > buf[i]) {
          buf[i] = right_buf[i];
        }
      }
    }

    dist <<= 1;
    round++;
  }

  spawn_free(&right_buf);
  spawn_free(&left_buf);

  return LWGRP_SUCCESS;
}

int lwgrp_allreduce_uint64_max(uint64_t* buf, uint64_t count, const lwgrp* group)
{
  int64_t rank  = group->rank;
  int64_t ranks = group->size;

  size_t buf_size = count * sizeof(uint64_t);
  uint64_t* left_buf  = SPAWN_MALLOC(buf_size);
  uint64_t* right_buf = SPAWN_MALLOC(buf_size);

  int i;
  int round = 0;
  int64_t dist = 1;
  while (dist < ranks) {
    /* send message left */
    if (rank - dist >= 0) {
      spawn_net_channel* ch = group->list_left[round];
      spawn_net_write(ch, buf, buf_size);
    }

    /* send message right */
    if (rank + dist < ranks) {
      spawn_net_channel* ch = group->list_right[round];
      spawn_net_write(ch, buf, buf_size);
    }

    /* recv message from left */
    if (rank - dist >= 0) {
      spawn_net_channel* ch = group->list_left[round];
      spawn_net_read(ch, left_buf, buf_size);
    }

    /* recv message from right */
    if (rank + dist < ranks) {
      spawn_net_channel* ch = group->list_right[round];
      spawn_net_read(ch, right_buf, buf_size);
    }

    /* merge received maps with our current map */
    if (rank - dist >= 0) {
      for (i = 0; i < count; i++) {
        if (left_buf[i] > buf[i]) {
          buf[i] = left_buf[i];
        }
      }
    }
    if (rank + dist < ranks) {
      for (i = 0; i < count; i++) {
        if (right_buf[i] > buf[i]) {
          buf[i] = right_buf[i];
        }
      }
    }

    dist <<= 1;
    round++;
  }

  spawn_free(&right_buf);
  spawn_free(&left_buf);

  return LWGRP_SUCCESS;
}

/* compute prefix sum across procs of a vector of uint64_t values */
int lwgrp_scan_uint64_sum_blocking(uint64_t* buf, uint64_t count, const lwgrp* group)
{
  int64_t rank  = group->rank;
  int64_t ranks = group->size;

  size_t buf_size = count * sizeof(uint64_t);
  uint64_t* recv_buf = SPAWN_MALLOC(buf_size);

  int i;
  int round = 0;
  int64_t dist = 1;
  while (dist < ranks) {
    /* compute our relative rank for this round */
    int64_t relrank  = (rank >> round);

    if (relrank & 0x1) {
      /* we're odd in this round */

      /* exchange data with left partner */
      if (rank - dist >= 0) {
        spawn_net_channel* ch = group->list_left[round];
        spawn_net_read(ch, recv_buf, buf_size);
      }

      /* exchange data with right partner */
      if (rank + dist < ranks) {
        spawn_net_channel* ch = group->list_right[round];
        spawn_net_write(ch, buf, buf_size);
      }
    } else {
      /* we're even in this round */

      /* exchange data with right partner */
      if (rank + dist < ranks) {
        spawn_net_channel* ch = group->list_right[round];
        spawn_net_write(ch, buf, buf_size);
      }

      /* exchange data with left partner */
      if (rank - dist >= 0) {
        spawn_net_channel* ch = group->list_left[round];
        spawn_net_read(ch, recv_buf, buf_size);
      }
    }

    /* merge received maps with our current map */
    if (rank - dist >= 0) {
      for (i = 0; i < count; i++) {
        buf[i] += recv_buf[i];
      }
    }

    dist <<= 1;
    round++;
  }

  spawn_free(&recv_buf);

  return LWGRP_SUCCESS;
}

/* compute prefix sum across procs of a vector of uint64_t values */
int lwgrp_scan_uint64_sum(uint64_t* buf, uint64_t count, const lwgrp* group)
{
  int64_t rank  = group->rank;
  int64_t ranks = group->size;

  size_t buf_size = count * sizeof(uint64_t);
  uint64_t* recv_buf = SPAWN_MALLOC(buf_size);

  int i;
  int round = 0;
  int64_t dist = 1;
  while (dist < ranks) {
    /* exchange data with right partner */
    if (rank + dist < ranks) {
      spawn_net_channel* ch = group->list_right[round];
      spawn_net_write(ch, buf, buf_size);
    }

    /* exchange data with left partner */
    if (rank - dist >= 0) {
      spawn_net_channel* ch = group->list_left[round];
      spawn_net_read(ch, recv_buf, buf_size);
    }

    /* merge received maps with our current map */
    if (rank - dist >= 0) {
      for (i = 0; i < count; i++) {
        buf[i] += recv_buf[i];
      }
    }

    dist <<= 1;
    round++;
  }

  spawn_free(&recv_buf);

  return LWGRP_SUCCESS;
}

int lwgrp_double_scan_uint64_sum_blocking(const uint64_t* buf, uint64_t* ltr, uint64_t* rtl, uint64_t count, const lwgrp* group)
{
  int64_t rank  = group->rank;
  int64_t ranks = group->size;

  size_t buf_size = count * sizeof(uint64_t);
  uint64_t* left_send  = SPAWN_MALLOC(buf_size);
  uint64_t* left_recv  = SPAWN_MALLOC(buf_size);
  uint64_t* right_send = SPAWN_MALLOC(buf_size);
  uint64_t* right_recv = SPAWN_MALLOC(buf_size);

  /* initialize outgoing buffers */
  int i;
  for (i = 0; i < count; i++) {
      left_send[i]  = buf[i];
      right_send[i] = buf[i];
  }

  int round = 0;
  int64_t dist = 1;
  while (dist < ranks) {
    /* compute our relative rank for this round */
    int64_t relrank  = (rank >> round);

    if (relrank & 0x1) {
      /* we're odd in this round */

      /* exchange data with left partner */
      if (rank - dist >= 0) {
        /* recv then send */
        spawn_net_channel* ch = group->list_left[round];
        spawn_net_read(ch, left_recv, buf_size);
        spawn_net_write(ch, left_send, buf_size);
      }

      /* exchange data with right partner */
      if (rank + dist < ranks) {
        /* send then recv */
        spawn_net_channel* ch = group->list_right[round];
        spawn_net_write(ch, right_send, buf_size);
        spawn_net_read(ch, right_recv, buf_size);
      }
    } else {
      /* we're even in this round */

      /* exchange data with right partner */
      if (rank + dist < ranks) {
        /* send then recv */
        spawn_net_channel* ch = group->list_right[round];
        spawn_net_write(ch, right_send, buf_size);
        spawn_net_read(ch, right_recv, buf_size);
      }

      /* exchange data with left partner */
      if (rank - dist >= 0) {
        /* recv then send */
        spawn_net_channel* ch = group->list_left[round];
        spawn_net_read(ch, left_recv, buf_size);
        spawn_net_write(ch, left_send, buf_size);
      }
    }

    /* merge received maps with our current map */
    if (rank - dist >= 0) {
      for (i = 0; i < count; i++) {
        right_send[i] += left_recv[i];
      }
    }
    if (rank + dist < ranks) {
      for (i = 0; i < count; i++) {
        left_send[i] += right_recv[i];
      }
    }

    dist <<= 1;
    round++;
  }

  for (i = 0; i < count; i++) {
    ltr[i] = right_send[i];
    rtl[i] = left_send[i];
  }

  spawn_free(&right_recv);
  spawn_free(&right_send);
  spawn_free(&left_recv);
  spawn_free(&left_send);

  return LWGRP_SUCCESS;
}

int lwgrp_double_scan_uint64_sum(const uint64_t* buf, uint64_t* ltr, uint64_t* rtl, uint64_t count, const lwgrp* group)
{
  int64_t rank  = group->rank;
  int64_t ranks = group->size;

  size_t buf_size = count * sizeof(uint64_t);
  uint64_t* left_send  = SPAWN_MALLOC(buf_size);
  uint64_t* left_recv  = SPAWN_MALLOC(buf_size);
  uint64_t* right_send = SPAWN_MALLOC(buf_size);
  uint64_t* right_recv = SPAWN_MALLOC(buf_size);

  /* initialize outgoing buffers */
  int i;
  for (i = 0; i < count; i++) {
      left_send[i]  = buf[i];
      right_send[i] = buf[i];
  }

  int round = 0;
  int64_t dist = 1;
  while (dist < ranks) {
    /* send data left */
    if (rank - dist >= 0) {
      spawn_net_channel* ch = group->list_left[round];
      spawn_net_write(ch, left_send, buf_size);
    }

    /* send data right */
    if (rank + dist < ranks) {
      /* send then recv */
      spawn_net_channel* ch = group->list_right[round];
      spawn_net_write(ch, right_send, buf_size);
    }

    /* recv data from left */
    if (rank - dist >= 0) {
      spawn_net_channel* ch = group->list_left[round];
      spawn_net_read(ch, left_recv, buf_size);
    }

    /* recv data from right */
    if (rank + dist < ranks) {
      spawn_net_channel* ch = group->list_right[round];
      spawn_net_read(ch, right_recv, buf_size);
    }

    /* merge received maps with our current map */
    if (rank - dist >= 0) {
      for (i = 0; i < count; i++) {
        right_send[i] += left_recv[i];
      }
    }
    if (rank + dist < ranks) {
      for (i = 0; i < count; i++) {
        left_send[i] += right_recv[i];
      }
    }

    dist <<= 1;
    round++;
  }

  for (i = 0; i < count; i++) {
    ltr[i] = right_send[i];
    rtl[i] = left_send[i];
  }

  spawn_free(&right_recv);
  spawn_free(&right_send);
  spawn_free(&left_recv);
  spawn_free(&left_send);

  return LWGRP_SUCCESS;
}

int lwgrp_allgather_strmap_blocking(strmap* map, const lwgrp* group)
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

int lwgrp_allgather_strmap(strmap* map, const lwgrp* group)
{
  int64_t rank  = group->rank;
  int64_t ranks = group->size;

  int round = 0;
  int64_t dist = 1;
  while (dist < ranks) {
    /* create temps to hold maps received from left and right */
    strmap* map_left = strmap_new();
    strmap* map_right = strmap_new();

    /* send map left */
    if (rank - dist >= 0) {
      spawn_net_channel* ch = group->list_left[round];
      spawn_net_write_strmap(ch, map);
    }

    /* send map right */
    if (rank + dist < ranks) {
      spawn_net_channel* ch = group->list_right[round];
      spawn_net_write_strmap(ch, map);
    }

    /* recv map from left */
    if (rank - dist >= 0) {
      spawn_net_channel* ch = group->list_left[round];
      spawn_net_read_strmap(ch, map_left);
    }

    /* recv map from right */
    if (rank + dist < ranks) {
      spawn_net_channel* ch = group->list_right[round];
      spawn_net_read_strmap(ch, map_right);
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
