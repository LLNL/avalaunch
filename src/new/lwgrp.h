#ifndef LWGRP_H
#define LWGRP_H

#include "spawn_net.h"
#include "strmap.h"

#ifdef __cplusplus
extern "C" {
#endif

/* Based on "Exascale Algorithms for Generalized MPI_Comm_split",
 * EuroMPI 2011, Adam Moody, Dong H. Ahn, and Bronis R. de Supinkski
 *
 * Executes an MPI_Comm_split operation using bitonic sort, a double
 * inclusive scan to find color boundaries and left and right group
 * neighbors, a recv from ANY_SOURCE, and a barrier, returns the
 * output group as a chain. */

#define LWGRP_SUCCESS (0)
#define LWGRP_FAILURE (1)

/* keeps list of connections to procs 2^d hops away
 * on left and right sides, truncates at ends */
typedef struct lwgrp {
  int64_t size;      /* number of processes in our group */
  int64_t rank;      /* our rank within the group [0,size) */
  char* name;        /* strdup of address of current process */
  char* left;        /* strdup of address of process one less than current */
  char* right;       /* strdup of address of process one more than current */
  int64_t list_size; /* number of elements in channel lists */
  spawn_net_channel** list_left;  /* process addresses for 2^d hops to the left */
  spawn_net_channel** list_right; /* process addresses for 2^d hops to the right */
  spawn_net_endpoint* ep; /* pointer to endpoint to accept connections */
} lwgrp;

/* create and returns a group given group size, rank, and spawn_net info */
lwgrp* lwgrp_create(
  int64_t size, /* number of ranks in group */
  int64_t rank, /* our rank within the group in range [0,size) */
  const char* name, /* our endpoint name */
  const char* left, /* endpoint name of rank one less (ignored if proc is first) */
  const char* right,/* endpoint name of rank one more (ignored if proc is last) */
  spawn_net_endpoint* ep /* our endpoint on which to accept connections */
);

/* split a group into groups consisting of all procs with the same string */
lwgrp* lwgrp_split_str(
  const lwgrp* comm, /* input group */
  const char* str    /* string on which to split procs (color), can't be NULL */
);

/* split a group into groups consisting of all procs with the same color,
 * order by key and rank in comm */
lwgrp* lwgrp_split(
  const lwgrp* comm, /* input group */
  int64_t color,     /* color value */
  int64_t key        /* key value */
);

/* free a group and drop connections */
int lwgrp_free(lwgrp** pgroup);

/* return size of group */
int64_t lwgrp_size(const lwgrp* group);

/* return rank of process within its group [0,size) */
int64_t lwgrp_rank(const lwgrp* group);

/* send data in buffer to ranks on left and right sides,
 * return value from left and right ranks */
int lwgrp_shift(const void* buf, void* left, void* right, size_t buf_size, const lwgrp* group);

/* execute a barrier on the group */
int lwgrp_barrier(const lwgrp* group);

/* compute sum across procs of a vector of uint64_t values */
int lwgrp_allreduce_uint64_sum(uint64_t* buf, uint64_t count, const lwgrp* group);

/* compute maximum across procs of a vector of uint64_t values */
int lwgrp_allreduce_uint64_max(uint64_t* buf, uint64_t count, const lwgrp* group);

/* compute prefix sum across procs of a vector of uint64_t values */
int lwgrp_scan_uint64_sum(uint64_t* buf, uint64_t count, const lwgrp* group);

/* compute prefix sum across procs of a vector of uint64_t values */
int lwgrp_double_scan_uint64_sum(const uint64_t* buf, uint64_t* left, uint64_t* right, uint64_t count, const lwgrp* group);

/* gather strmap from all procs */
int lwgrp_allgather_strmap(strmap* map, const lwgrp* group);

#ifdef __cplusplus
}
#endif

#endif /* LWGRP_H */
