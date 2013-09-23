#include <string.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netdb.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

#include <child.h>

extern struct child_s *
launch (char * const location)
{
    int islocal = is_local_ipaddr(location);
    printf("%s is %slocal\n", location, islocal ? "" : "not ");
    if (islocal) {
        char const * command[] = {
            "echo",
            "foo",
            "bar",
            NULL
        };

        return create_child(command);
    }

    else {
        char const * command[] = {
            "ssh",
            location,
            "hostname",
            NULL
        };

        return create_child(command);
    }
}
