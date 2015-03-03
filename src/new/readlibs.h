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

#ifndef READLIBS_H_
#define READLIBS_H_

#include "spawn.h"

/* given a path to an executable, lookup its list of libraries
 * and record the full path to each in the given map */
int lib_capture(strmap* map, const char* file);

#endif /* READLIBS_H_ */
