#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>

#include <is_local_ipaddr.h>
#include <event_handler.h>
#include <node.h>

int
main (int argc, char * argv[])
{
    int i, n;

    is_local_ipaddr_db_init();
    if (start_event_handler()) {
        exit(EXIT_FAILURE);
    }

    if (node_initialize()) {
        exit(EXIT_FAILURE);
    }

    for (i = 1, n = 0; i < argc; i++) {
        if (0 > node_get_id(argv[i])) {
            exit(EXIT_FAILURE);
        }
    }

    for (i = 0, n = node_count(); i < n; i++) {
        node_launch(i);
    }

    while (n > get_num_exited());

    stop_event_handler();
    node_finalize();
    is_local_ipaddr_db_free();

    return EXIT_SUCCESS;
}
