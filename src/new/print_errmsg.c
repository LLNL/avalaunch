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

#include <errno.h>
#include <stdio.h>
#include <pthread.h>

/*
 * Thread safe way to print error message
 */
extern void
print_errmsg (char * prefix, int error)
{
    static pthread_mutex_t mutex = PTHREAD_MUTEX_INITIALIZER;

    pthread_mutex_lock(&mutex);
    fprintf(stderr, "%s: %s\n", prefix, strerror(error));
    pthread_mutex_unlock(&mutex);
}
