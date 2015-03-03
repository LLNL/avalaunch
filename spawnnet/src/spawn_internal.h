/*
 * Copyright (c) 2015, Lawrence Livermore National Security, LLC.
 * Produced at the Lawrence Livermore National Laboratory.
 * Written by Adam Moody <moody20@llnl.gov>.
 * LLNL-CODE-667277.
 * All rights reserved.
 * This file is part of the SpawnNet library.
 * For details, see https://github.com/hpc/spawnnet
 * Please also read this file: LICENSE.TXT.
*/

#ifndef SPAWN_INTERNAL_H
#define SPAWN_INTERNAL_H

#include "spawn_util.h"
#include "strmap.h"
#include "spawn_net.h"
#include "spawn_net_tcp.h"
#include "spawn_net_fifo.h"

#ifdef HAVE_SPAWN_NET_IBUD
#include "spawn_net_ib.h"
#endif

#include "spawn_net_util.h"

#define SPAWN_SUCCESS (0)
#define SPAWN_FAILURE (1)

#endif /* SPAWN_INTERNAL_H */
