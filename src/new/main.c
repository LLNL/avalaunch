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

/*
 * Local Headers
 */
#include <session.h>

/*
 * System Headers
 */
#include <stdlib.h>
#include <stdio.h>

int
main (int argc, char * argv[])
{
    struct session_t * my_session = session_init(argc, argv);
    int nodeid = get_spawn_id(my_session);
    int tid;

    if (my_session) {
        if (!nodeid) { tid = begin_delta("session_start"); }
        session_start(my_session);
        if (!nodeid) { end_delta(tid); }

        if (!nodeid) { tid = begin_delta("session_destroy"); }
        session_destroy(my_session);
        if (!nodeid) { end_delta(tid); }
    }

    else {
        exit(EXIT_FAILURE);
    }

    if (!nodeid) { print_deltas(stdout); }
    return EXIT_SUCCESS;
}
