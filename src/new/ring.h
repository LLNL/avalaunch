/*
 * Copyright (c) 2015, Lawrence Livermore National Security, LLC.
 * Produced at the Lawrence Livermore National Laboratory.
 * Written by Adam Moody <moody20@llnl.gov>.
 * LLNL-CODE-667270.
 * All rights reserved.
 * This file is part of the Avalaunch process launcher.
 * For details, see https://github.com/hpc/avalaunch
 * Please also read the LICENSE file.
*/

#ifndef RING_H
#define RING_H

#if defined(__cplusplus)
extern "C" {
#endif

#include <stdlib.h>
#include <stdint.h>

#define RING_SUCCESS (0)
#define RING_FAILURE (1)

int ring_create(
  const char* addr, /* IN  - address of caller */
  uint64_t* rank,   /* OUT - rank of caller */
  uint64_t* ranks,  /* OUT - number of ranks in job */
  char** left,      /* OUT - address of left rank, caller should free() */
  char** right      /* OUT - address of right rank, caller should free() */
);

int ring_create2(
  const char* addr, /* IN  - address of caller */
  uint64_t* rank,   /* OUT - rank of caller */
  uint64_t* ranks,  /* OUT - number of ranks in job */
  char** left,      /* OUT - address of left rank, caller should free() */
  char** right      /* OUT - address of right rank, caller should free() */
);

#if defined(__cplusplus)
}
#endif

#endif /* RING_H */
