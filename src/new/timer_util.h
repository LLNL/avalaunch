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

#ifndef SPAWN_TIMER_H
#define SPAWN_TIMER_H 1

#include <stdio.h>

int begin_delta (const char * label);
void end_delta (int delta_id);
void print_deltas (FILE * fd);

#endif
