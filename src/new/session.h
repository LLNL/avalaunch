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

#ifndef SESSION_H
#define SESSION_H 1

struct session_t;

struct session_t * session_init (int argc, char * argv[]);
int get_spawn_id (struct session_t *);
int session_start (struct session_t *);
void session_destroy (struct session_t *);

#endif
