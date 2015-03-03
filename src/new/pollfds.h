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

#ifndef POLLFDS_H
#define POLLFDS_H 1

#include <stdlib.h>

struct pollfds_param {
    int fd;
    void (* fd_handler)(size_t, int);
};

extern int pollfds_add (size_t id, struct pollfds_param stdin_param, struct
        pollfds_param stdout_param, struct pollfds_param stderr_param);
extern void pollfds_poll (void);
extern void pollfds_process (void);

#endif
