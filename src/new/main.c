/*
 * Local Headers
 */
#include <session.h>

/*
 * System Headers
 */
#include <stdlib.h>

int
main (int argc, char * argv[])
{
    struct session_t * my_session = session_init(argc, argv);

    if (my_session) {
        session_start(my_session);
        session_destroy(my_session);
    }

    else {
        exit(EXIT_FAILURE);
    }

    return EXIT_SUCCESS;
}
