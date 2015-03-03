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

#ifndef NODE_H
#define NODE_H 1

int node_initialize ();
int node_finalize (void);
int node_get_id (char const * location);
int node_launch (size_t id, const char* cmd);

#endif
