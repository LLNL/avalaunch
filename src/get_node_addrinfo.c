#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netdb.h>
#include <stdio.h>

static int
get_node_addrinfo (char const *addr, char ip[NI_MAXHOST], char
        hostname[NI_MAXHOST])
{
    struct addrinfo hints;
    struct addrinfo *result, *rp;
    int s;

    memset(&hints, 0, sizeof (struct addrinfo));
    hints.ai_family = AF_UNSPEC;
    hints.ai_protocol = 0;

    s = getaddrinfo(addr, NULL, &hints, &result);

    if (0 != s) {
        fprintf(stderr, "getaddrinfo: %s\n", gai_strerror(s));
        return -1;
    }

    s = getnameinfo(result->ai_addr, result->ai_addrlen, ip, sizeof(ip), NULL,
            0, NI_NUMERICHOST);

    if (0 != s) {
        fprintf(stderr, "getnameinfo() failed: %s\n", gai_strerror(s));
        freeaddrinfo(result);
        return -1;
    }

    s = getnameinfo(result->ai_addr, result->ai_addrlen, hostname,
            sizeof(hostname), NULL, 0, 0);

    if (0 != s) {
        fprintf(stderr, "getnameinfo() failed: %s\n", gai_strerror(s));
        freeaddrinfo(result);
        return -1;
    }

    freeaddrinfo(result);
    return 0;
}
