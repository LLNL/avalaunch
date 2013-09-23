#include <arpa/inet.h>
#include <sys/socket.h>
#include <netdb.h>
#include <ifaddrs.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <search.h>

extern int
is_local_ipaddr (char const * ipaddr)
{
    struct addrinfo hints;
    struct addrinfo *result, *rp;
    char node[NI_MAXHOST];
    ENTRY e;
    int s;

    memset(&hints, 0, sizeof (struct addrinfo));
    hints.ai_family = AF_UNSPEC;
    hints.ai_protocol = 0;

    s = getaddrinfo(ipaddr, NULL, &hints, &result);
    if (0 != s) {
        fprintf(stderr, "getaddrinfo: %s\n", gai_strerror(s));
        return 0;
    }

    s = getnameinfo(result->ai_addr, result->ai_addrlen, node, sizeof(node),
            NULL, 0, NI_NUMERICHOST);
    if (0 != s) {
        fprintf(stderr, "getnameinfo() failed: %s\n", gai_strerror(s));
        return 0;
    }

    e.key = node;
    return NULL != hsearch(e, FIND);
}

int
is_local_ipaddr_db_free (void)
{
    hdestroy();
}

int
is_local_ipaddr_db_init (void)
{
    struct ifaddrs *ifaddr, *ifa;
    int s;

    if(-1 == getifaddrs(&ifaddr)) {
        perror("getifaddrs");
        return -1;
    }

    hcreate(16);

    for (ifa = ifaddr; ifa != NULL; ifa = ifa->ifa_next) {
        int family;
        char ipaddr[NI_MAXHOST];
        ENTRY e, *ep;

        if (ifa->ifa_addr == NULL) continue;

        family = ifa->ifa_addr->sa_family;
        if (AF_INET != family && AF_INET6 != family) continue;

        s = getnameinfo(ifa->ifa_addr, (family == AF_INET) ? sizeof(struct
                    sockaddr_in) : sizeof(struct sockaddr_in6), ipaddr,
                NI_MAXHOST, NULL, 0, NI_NUMERICHOST);
        if (s != 0) {
            printf("getnameinfo() failed: %s\n", gai_strerror(s));
            continue;
        }

        e.key = strdup(ipaddr);

        if (NULL == hsearch(e, ENTER)) {
            perror("hsearch");
        }
    }

    freeifaddrs(ifaddr);
}

