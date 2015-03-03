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

#ifndef SPAWN_NET_UTIL_H
#define SPAWN_NET_UTIL_H

#include "strmap.h"
#include "spawn_net.h"

#ifdef __cplusplus
extern "C" {
#endif

/* write string to spawn_net channel */
void spawn_net_write_str(const spawn_net_channel* ch, const char* str);

/* read string from spawn_net channel, returns newly allocated string */
char* spawn_net_read_str(const spawn_net_channel* ch);

/* pack specified strmap and write it to channel */
void spawn_net_write_strmap(const spawn_net_channel* ch, const strmap* map);

/* read packed strmap from channel into specified map */
void spawn_net_read_strmap(const spawn_net_channel* ch, strmap* map);

#ifdef __cplusplus
}
#endif
#endif /* SPAWN_NET_UTIL_H */
